defmodule KafkaEx.Client.ConsumerGroupProtocolTest do
  @moduledoc """
  ConsumerGroupHeartbeat / ConsumerGroupDescribe (KIP-848) through a started
  client on the fake transport: version negotiation, coordinator routing and
  re-discovery, retry classification, assignment topic-name resolution and
  telemetry.
  """
  use KafkaEx.TestSupport.PinCase, async: false

  alias KafkaEx.Messages.ConsumerGroupDescription
  alias KafkaEx.Messages.ConsumerGroupHeartbeat
  alias KafkaEx.Messages.TopicPartitions

  @moduletag pin_set: :invariant

  @known_id <<1::128>>
  @unknown_id <<2::128>>

  setup context do
    versions =
      Map.merge(
        %{consumer_group_heartbeat: 0..1, consumer_group_describe: 0..1, metadata: 10..10},
        Map.get(context, :api_versions, %{})
      )

    {:ok,
     start_pinned(
       api_versions: versions,
       topics: %{},
       fake: Map.get(context, :fake, []),
       client: [consumer_group: "g"],
       before_boot: &FakeTransport.respond(&1, :metadata, fn request -> orders_metadata(request.host) end)
     )}
  end

  # --- wire fixtures (response header v1: correlation id + empty tagged fields) ---

  defp heartbeat_response(correlation_id, opts) do
    error_code = Keyword.get(opts, :error_code, 0)
    member_id = Keyword.get(opts, :member_id, "m-1")
    epoch = Keyword.get(opts, :member_epoch, 1)

    assignment =
      case Keyword.get(opts, :assignment) do
        nil ->
          <<255>>

        topics ->
          entries =
            for {id, partitions} <- topics, into: <<>> do
              id <> <<length(partitions) + 1>> <> for(p <- partitions, into: <<>>, do: <<p::32>>) <> <<0>>
            end

          <<1, length(topics) + 1>> <> entries <> <<0>>
      end

    <<correlation_id::32, 0, 0::32, error_code::16, 0, byte_size(member_id) + 1>> <>
      member_id <> <<epoch::32-signed, 5_000::32>> <> assignment <> <<0>>
  end

  defp describe_response(correlation_id, group_id, error_code) do
    # throttle, 1 group: error_code, error_message null, group_id, state "", epochs 0, assignor "",
    # no members, authorized_operations, tagged fields; top-level tagged fields
    <<correlation_id::32, 0, 0::32, 2, error_code::16, 0, byte_size(group_id) + 1>> <>
      group_id <> <<1, 0::32, 0::32, 1, 1, 0::32, 0, 0>>
  end

  defp orders_metadata(host) do
    %{
      brokers: [%{node_id: 1, host: host, port: 9092}, %{node_id: 2, host: host, port: 9093}],
      topics: [
        %{
          name: "orders",
          topic_id: @known_id,
          partitions: [%{partition_index: 0, leader_id: 1, replica_nodes: [1], isr_nodes: [1]}]
        }
      ]
    }
  end

  defp respond_heartbeat(fake, opts),
    do: FakeTransport.respond(fake, :consumer_group_heartbeat, &{:frame, heartbeat_response(&1.correlation_id, opts)})

  defp heartbeat(client, group \\ "g", opts) do
    Client.request(client, {:consumer_group_heartbeat, group, opts}, 5_000)
  end

  # The first request of the API needs a FindCoordinator first; one after it is a re-discovery.
  defp apis_after_first(fake, api) do
    fake |> FakeTransport.requests() |> Enum.map(& &1.api) |> Enum.drop_while(&(&1 != api)) |> Enum.drop(1)
  end

  describe "consumer_group_heartbeat" do
    test "negotiates v1 and resolves assignment topic names from cluster metadata", %{fake: fake, client: client} do
      respond_heartbeat(fake, assignment: [{@known_id, [0, 1]}, {@unknown_id, [2]}])

      opts = [member_id: "m-1", member_epoch: 0, subscribed_topic_names: ["orders"], topic_partitions: []]

      assert {:ok, %ConsumerGroupHeartbeat{} = result} = heartbeat(client, opts)

      assert [%{api_version: 1}] = requests(fake, :consumer_group_heartbeat)
      assert result.member_id == "m-1"
      assert result.member_epoch == 1
      assert result.heartbeat_interval_ms == 5_000

      assert result.assignment == [
               %TopicPartitions{topic_id: @known_id, topic_name: "orders", partitions: [0, 1]},
               %TopicPartitions{topic_id: @unknown_id, topic_name: nil, partitions: [2]}
             ]
    end

    test "an unchanged assignment stays nil", %{fake: fake, client: client} do
      respond_heartbeat(fake, member_epoch: 4)

      assert {:ok, %ConsumerGroupHeartbeat{assignment: nil, member_epoch: 4}} =
               heartbeat(client, member_id: "m-1", member_epoch: 4)
    end

    test ":fenced_member_epoch is returned after a single attempt", %{fake: fake, client: client} do
      respond_heartbeat(fake, error_code: 110, member_epoch: -1)

      assert {:error, %{error: :fenced_member_epoch}} = heartbeat(client, member_id: "m-1", member_epoch: 3)

      assert count(fake, :consumer_group_heartbeat) == 1
    end

    test ":not_coordinator re-discovers the coordinator before retrying", %{fake: fake, client: client} do
      respond_heartbeat(fake, error_code: 16)

      with_log(fn -> heartbeat(client, member_id: "m-1", member_epoch: 3) end)

      assert :find_coordinator in apis_after_first(fake, :consumer_group_heartbeat)
    end

    test ":coordinator_load_in_progress is retried without re-discovery", %{fake: fake, client: client} do
      respond_heartbeat(fake, error_code: 14)

      {result, _log} = with_log(fn -> heartbeat(client, member_id: "m-1", member_epoch: 3) end)

      assert {:error, %{error: :coordinator_load_in_progress}} = result
      assert count(fake, :consumer_group_heartbeat) >= 2
      refute :find_coordinator in apis_after_first(fake, :consumer_group_heartbeat)
    end

    @tag api_versions: %{consumer_group_heartbeat: 0..0}
    test "subscribed_topic_regex with only v0 negotiated is rejected before sending", %{fake: fake, client: client} do
      sent_before = non_handshake(fake)

      assert {:error, :api_version_no_supported} =
               heartbeat(client, member_epoch: 0, subscribed_topic_regex: "o.*")

      assert non_handshake(fake) == sent_before
    end

    @tag api_versions: %{consumer_group_heartbeat: nil}
    test "a broker without the API returns :api_not_supported_by_broker", %{client: client} do
      assert {:error, :api_not_supported_by_broker} = heartbeat(client, member_epoch: 0)
    end

    test "malformed :topic_partitions is an error, not a crash in the client", %{fake: fake, client: client} do
      sent_before = non_handshake(fake)

      for bad <- [
            [%{topic: "t", partitions: [0]}],
            [%{topic_id: nil, partitions: [0]}],
            [%{topic_id: <<1::64>>, partitions: [0]}],
            [%{topic_id: @known_id, partitions: ["0"]}],
            %{topic_id: @known_id, partitions: [0]}
          ] do
        assert {:error, :invalid_topic_partitions} = heartbeat(client, member_epoch: 0, topic_partitions: bad)
      end

      assert non_handshake(fake) == sent_before
    end

    test "every malformed heartbeat field is an error, not a crash in the client", %{fake: fake, client: client} do
      # These used to reach Kayrock unchecked and raise InvalidRequestError
      # (or KeyError for a missing epoch) inside the shared client process.
      cases = [
        {[member_id: "m"], :invalid_member_epoch},
        {[member_epoch: nil], :invalid_member_epoch},
        {[member_epoch: 4_294_967_296], :invalid_member_epoch},
        {[member_epoch: 0, member_id: 123], :invalid_member_id},
        {[member_epoch: 0, instance_id: 5], :invalid_instance_id},
        {[member_epoch: 0, rack_id: :rack1], :invalid_rack_id},
        {[member_epoch: 0, rebalance_timeout_ms: "60000"], :invalid_rebalance_timeout_ms},
        {[member_epoch: 0, subscribed_topic_names: "orders"], :invalid_subscribed_topic_names},
        {[member_epoch: 0, subscribed_topic_names: [:orders]], :invalid_subscribed_topic_names},
        {[member_epoch: 0, subscribed_topic_regex: ~r/x/], :invalid_subscribed_topic_regex},
        {[member_epoch: 0, server_assignor: :uniform], :invalid_server_assignor}
      ]

      sent_before = non_handshake(fake)

      for {opts, expected} <- cases do
        assert {:error, ^expected} = heartbeat(client, opts), "expected #{inspect(opts)} -> #{inspect(expected)}"
      end

      assert non_handshake(fake) == sent_before
    end

    test "nil optional fields are allowed (KIP-848 'unchanged')", %{fake: fake, client: client, host: host} do
      FakeTransport.fail_send(fake, :consumer_group_heartbeat, 0, :request_timed_out)

      {result, log} =
        with_log(fn ->
          heartbeat(client,
            member_epoch: 3,
            member_id: "m",
            instance_id: nil,
            rack_id: nil,
            subscribed_topic_names: nil
          )
        end)

      assert {:error, _not_a_validation_error} = result
      assert failed_sends(log, host, :request_timed_out) >= 1
    end

    test "a non-integer :member_epoch is an error, not a crash in the client", %{fake: fake, client: client} do
      sent_before = non_handshake(fake)
      assert {:error, :invalid_member_epoch} = heartbeat(client, member_epoch: "0")
      assert non_handshake(fake) == sent_before
    end

    test "rejects an invalid group", %{client: client} do
      assert {:error, :invalid_consumer_group} = heartbeat(client, "", member_epoch: 0)
    end

    test "emits the consumer_group_heartbeat telemetry span", %{fake: fake, client: client} do
      test_pid = self()
      handler = "cgh-telemetry-#{inspect(self())}"

      :telemetry.attach_many(
        handler,
        [
          [:kafka_ex, :consumer, :consumer_group_heartbeat, :start],
          [:kafka_ex, :consumer, :consumer_group_heartbeat, :stop]
        ],
        fn event, _measurements, metadata, _ -> send(test_pid, {event, metadata}) end,
        nil
      )

      on_exit(fn -> :telemetry.detach(handler) end)

      respond_heartbeat(fake, member_id: "gen-id", member_epoch: 2, assignment: [{@known_id, [0, 1]}])

      heartbeat(client, member_epoch: 0, member_id: "gen-id")

      assert_receive {[:kafka_ex, :consumer, :consumer_group_heartbeat, :start], start}, 5_000
      assert %{group_id: "g", member_id: "gen-id", member_epoch: 0} = start

      assert_receive {[:kafka_ex, :consumer, :consumer_group_heartbeat, :stop], stop}, 5_000
      assert %{result: :ok, member_epoch: 0, new_member_epoch: 2, assigned_partitions: 2} = stop
    end
  end

  describe "consumer_group_heartbeat member id (KIP-1082)" do
    setup do
      test_pid = self()
      handler = "cgh-member-id-#{inspect(self())}"

      :telemetry.attach(
        handler,
        [:kafka_ex, :consumer, :consumer_group_heartbeat, :stop],
        fn _event, _measurements, metadata, _ -> send(test_pid, {:stop, metadata}) end,
        nil
      )

      on_exit(fn -> :telemetry.detach(handler) end)
      :ok
    end

    defp sent_member_id(frame) do
      # request header v2: api key, version, correlation id, client id (int16 string), tagged
      # fields; then group_id and member_id as compact strings
      <<_::16, _::16, _::32, client_id_len::16, _::binary-size(client_id_len), 0, rest::binary>> = frame

      <<group_len_plus_1, _::binary-size(group_len_plus_1 - 1), id_len_plus_1,
        member_id::binary-size(id_len_plus_1 - 1), _::binary>> = rest

      member_id
    end

    test "a failed v1 join without :member_id reports the generated id that was sent", %{fake: fake, client: client} do
      # The short timeout must cover only the muted heartbeat, not the coordinator lookup.
      FakeTransport.respond(fake, :consumer_group_describe, &{:frame, describe_response(&1.correlation_id, "g", 0)})
      assert {:ok, _} = Client.request(client, {:consumer_group_describe, ["g"], []}, 5_000)
      with_request_timeout(200)
      FakeTransport.respond(fake, :consumer_group_heartbeat, :no_reply)

      {result, _log} = with_log(fn -> heartbeat(client, member_epoch: 0, topic_partitions: []) end)

      assert {:error, _} = result
      assert [%{frame: frame}] = requests(fake, :consumer_group_heartbeat)
      sent_id = sent_member_id(frame)
      assert byte_size(sent_id) == 22
      assert_receive {:stop, %{result: :error, member_id: ^sent_id, member_epoch: 0}}, 5_000
    end

    test "a successful v1 join without :member_id reports the id echoed by the broker", %{fake: fake, client: client} do
      FakeTransport.respond(
        fake,
        :consumer_group_heartbeat,
        &{:frame, heartbeat_response(&1.correlation_id, member_id: sent_member_id(&1.frame), member_epoch: 1)}
      )

      assert {:ok, %ConsumerGroupHeartbeat{member_id: id}} = heartbeat(client, member_epoch: 0, topic_partitions: [])

      assert byte_size(id) == 22
      assert_receive {:stop, %{result: :ok, member_id: ^id}}, 5_000
    end

    test "a caller-supplied :member_id is sent and reported unchanged", %{fake: fake, client: client} do
      FakeTransport.fail_send(fake, :consumer_group_heartbeat, 0, :timeout)

      with_log(fn -> heartbeat(client, member_epoch: 0, member_id: "mine") end)

      assert_receive {:stop, %{result: :error, member_id: "mine"}}, 5_000
    end

    @tag fake: [api_versions_by_node: %{2 => %{consumer_group_heartbeat: 0..0}}]
    test "a join rebuilt at the coordinator's lower version keeps its generated member id across retries", %{
      fake: fake,
      client: client
    } do
      calls = :counters.new(1, [])

      FakeTransport.respond(fake, :consumer_group_heartbeat, fn request ->
        :counters.add(calls, 1, 1)

        if :counters.get(calls, 1) <= 2 do
          :close
        else
          {:frame,
           heartbeat_response(request.correlation_id, member_id: sent_member_id(request.frame), member_epoch: 1)}
        end
      end)

      {result, _log} = with_log(fn -> heartbeat(client, member_epoch: 0, topic_partitions: []) end)

      assert {:ok, _} = result
      sent = Enum.map(requests(fake, :consumer_group_heartbeat), &sent_member_id(&1.frame))
      assert length(sent) == 3
      assert [generated] = Enum.uniq(sent)
      assert byte_size(generated) == 22
    end
  end

  describe "consumer_group_describe" do
    test "an unknown group returns the group-level error", %{fake: fake, client: client} do
      FakeTransport.respond(fake, :consumer_group_describe, &{:frame, describe_response(&1.correlation_id, "g", 69)})

      # same shape as DescribeGroups: {group_id, error}; KafkaEx.API unwraps it
      assert {:error, {"g", :group_id_not_found}} = Client.request(client, {:consumer_group_describe, ["g"], []}, 5_000)
    end

    test "parses a described group", %{fake: fake, client: client} do
      FakeTransport.respond(fake, :consumer_group_describe, &{:frame, describe_response(&1.correlation_id, "g", 0)})

      assert {:ok, [%ConsumerGroupDescription{group_id: "g", type: :consumer, members: []}]} =
               Client.request(client, {:consumer_group_describe, ["g"], []}, 5_000)
    end

    test "a group-level :not_coordinator re-discovers the coordinator", %{fake: fake, client: client} do
      FakeTransport.respond(fake, :consumer_group_describe, &{:frame, describe_response(&1.correlation_id, "g", 16)})

      with_log(fn -> Client.request(client, {:consumer_group_describe, ["g"], []}, 5_000) end)

      assert :find_coordinator in apis_after_first(fake, :consumer_group_describe)
    end
  end
end
