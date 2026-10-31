defmodule KafkaEx.Client.ConsumerGroupProtocolTest do
  @moduledoc """
  ConsumerGroupHeartbeat / ConsumerGroupDescribe (KIP-848) through the real
  `KafkaEx.Client.handle_call/3` path, with the network seam stubbed: version
  negotiation, coordinator routing and re-discovery, retry classification,
  assignment topic-name resolution and telemetry.
  """
  use ExUnit.Case, async: false
  use Mimic

  alias KafkaEx.Client
  alias KafkaEx.Client.RequestBuilder
  alias KafkaEx.Client.State
  alias KafkaEx.Cluster.Broker
  alias KafkaEx.Cluster.ClusterMetadata
  alias KafkaEx.Cluster.Topic
  alias KafkaEx.Messages.ConsumerGroupDescription
  alias KafkaEx.Messages.ConsumerGroupHeartbeat
  alias KafkaEx.Messages.TopicPartitions
  alias KafkaEx.Network.NetworkClient
  alias KafkaEx.Network.Socket

  @heartbeat_api_key 68
  @describe_api_key 69
  @known_id <<1::128>>
  @unknown_id <<2::128>>

  setup :set_mimic_private

  setup do
    {:ok, listen} = :gen_tcp.listen(0, [:binary, active: false])
    {:ok, lport} = :inet.port(listen)
    {:ok, sock} = :gen_tcp.connect(~c"localhost", lport, [:binary, active: false])

    on_exit(fn ->
      :gen_tcp.close(sock)
      :gen_tcp.close(listen)
    end)

    broker = %Broker{node_id: 1, host: "localhost", port: lport, socket: %Socket{socket: sock, ssl: false}}

    state = %State{
      consumer_group_for_auto_commit: "g",
      api_versions: %{@heartbeat_api_key => {0, 1}, @describe_api_key => {0, 1}},
      correlation_id: 1,
      cluster_metadata: %ClusterMetadata{
        brokers: %{1 => broker},
        consumer_group_coordinators: %{"g" => 1},
        topics: %{"orders" => %Topic{name: "orders", topic_id: @known_id}}
      }
    }

    {:ok, state: state}
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

  defp correlation_id(wire) do
    <<_api_key::16, _version::16, correlation_id::32, _::binary>> = IO.iodata_to_binary(wire)
    correlation_id
  end

  defp api_version(wire) do
    <<_api_key::16, version::16, _::binary>> = IO.iodata_to_binary(wire)
    version
  end

  describe "consumer_group_heartbeat" do
    test "negotiates v1 and resolves assignment topic names from cluster metadata", %{state: state} do
      test_pid = self()

      stub(NetworkClient, :send_sync_request, fn _broker, wire, _timeout ->
        send(test_pid, {:version, api_version(wire)})
        heartbeat_response(correlation_id(wire), assignment: [{@known_id, [0, 1]}, {@unknown_id, [2]}])
      end)

      opts = [member_id: "m-1", member_epoch: 0, subscribed_topic_names: ["orders"], topic_partitions: []]

      assert {:reply, {:ok, %ConsumerGroupHeartbeat{} = result}, _state} =
               Client.handle_call({:consumer_group_heartbeat, "g", opts}, self(), state)

      assert_received {:version, 1}
      assert result.member_id == "m-1"
      assert result.member_epoch == 1
      assert result.heartbeat_interval_ms == 5_000

      assert result.assignment == [
               %TopicPartitions{topic_id: @known_id, topic_name: "orders", partitions: [0, 1]},
               %TopicPartitions{topic_id: @unknown_id, topic_name: nil, partitions: [2]}
             ]
    end

    test "an unchanged assignment stays nil", %{state: state} do
      stub(NetworkClient, :send_sync_request, fn _broker, wire, _timeout ->
        heartbeat_response(correlation_id(wire), member_epoch: 4)
      end)

      assert {:reply, {:ok, %ConsumerGroupHeartbeat{assignment: nil, member_epoch: 4}}, _} =
               Client.handle_call({:consumer_group_heartbeat, "g", [member_id: "m-1", member_epoch: 4]}, self(), state)
    end

    test ":fenced_member_epoch is returned after a single attempt", %{state: state} do
      test_pid = self()

      stub(NetworkClient, :send_sync_request, fn _broker, wire, _timeout ->
        send(test_pid, :sent)
        heartbeat_response(correlation_id(wire), error_code: 110, member_epoch: -1)
      end)

      assert {:reply, {:error, %{error: :fenced_member_epoch}}, _} =
               Client.handle_call({:consumer_group_heartbeat, "g", [member_id: "m-1", member_epoch: 3]}, self(), state)

      assert_received :sent
      refute_received :sent
    end

    test ":not_coordinator re-discovers the coordinator before retrying", %{state: state} do
      test_pid = self()

      stub(NetworkClient, :send_sync_request, fn _broker, wire, _timeout ->
        heartbeat_response(correlation_id(wire), error_code: 16)
      end)

      stub(RequestBuilder, :find_coordinator_request, fn _opts, _state ->
        send(test_pid, :rediscovered)
        {:error, :api_version_no_supported}
      end)

      Client.handle_call({:consumer_group_heartbeat, "g", [member_id: "m-1", member_epoch: 3]}, self(), state)

      assert_received :rediscovered
    end

    test ":coordinator_load_in_progress is retried without re-discovery", %{state: state} do
      test_pid = self()

      stub(NetworkClient, :send_sync_request, fn _broker, wire, _timeout ->
        send(test_pid, :sent)
        heartbeat_response(correlation_id(wire), error_code: 14)
      end)

      stub(RequestBuilder, :find_coordinator_request, fn _opts, _state ->
        send(test_pid, :rediscovered)
        {:error, :api_version_no_supported}
      end)

      assert {:reply, {:error, %{error: :coordinator_load_in_progress}}, _} =
               Client.handle_call({:consumer_group_heartbeat, "g", [member_id: "m-1", member_epoch: 3]}, self(), state)

      assert_received :sent
      assert_received :sent
      refute_received :rediscovered
    end

    test "subscribed_topic_regex with only v0 negotiated is rejected before sending", %{state: state} do
      state = %{state | api_versions: %{@heartbeat_api_key => {0, 0}}}
      reject(&NetworkClient.send_sync_request/3)

      assert {:reply, {:error, :api_version_no_supported}, _} =
               Client.handle_call(
                 {:consumer_group_heartbeat, "g", [member_epoch: 0, subscribed_topic_regex: "o.*"]},
                 self(),
                 state
               )
    end

    test "a broker without the API returns :api_not_supported_by_broker", %{state: state} do
      state = %{state | api_versions: %{}}

      assert {:reply, {:error, :api_not_supported_by_broker}, _} =
               Client.handle_call({:consumer_group_heartbeat, "g", [member_epoch: 0]}, self(), state)
    end

    test "malformed :topic_partitions is an error, not a crash in the client", %{state: state} do
      reject(&NetworkClient.send_sync_request/3)

      for bad <- [
            [%{topic: "t", partitions: [0]}],
            [%{topic_id: nil, partitions: [0]}],
            [%{topic_id: <<1::64>>, partitions: [0]}],
            [%{topic_id: @known_id, partitions: ["0"]}],
            %{topic_id: @known_id, partitions: [0]}
          ] do
        assert {:reply, {:error, :invalid_topic_partitions}, ^state} =
                 Client.handle_call(
                   {:consumer_group_heartbeat, "g", [member_epoch: 0, topic_partitions: bad]},
                   self(),
                   state
                 )
      end
    end

    test "every malformed heartbeat field is an error, not a crash in the client", %{state: state} do
      # These used to reach Kayrock unchecked and raise InvalidRequestError
      # (or KeyError for a missing epoch) inside the shared client process.
      reject(&NetworkClient.send_sync_request/3)

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

      for {opts, expected} <- cases do
        assert {:reply, {:error, ^expected}, ^state} =
                 Client.handle_call({:consumer_group_heartbeat, "g", opts}, self(), state),
               "expected #{inspect(opts)} -> #{inspect(expected)}"
      end
    end

    test "nil optional fields are allowed (KIP-848 'unchanged')", %{state: state} do
      stub(NetworkClient, :send_sync_request, fn _broker, _wire, _timeout -> {:error, :request_timed_out} end)

      assert {:reply, {:error, _not_a_validation_error}, _} =
               Client.handle_call(
                 {:consumer_group_heartbeat, "g",
                  [member_epoch: 3, member_id: "m", instance_id: nil, rack_id: nil, subscribed_topic_names: nil]},
                 self(),
                 state
               )
    end

    test "a non-integer :member_epoch is an error, not a crash in the client", %{state: state} do
      reject(&NetworkClient.send_sync_request/3)

      assert {:reply, {:error, :invalid_member_epoch}, ^state} =
               Client.handle_call({:consumer_group_heartbeat, "g", [member_epoch: "0"]}, self(), state)
    end

    test "rejects an invalid group", %{state: state} do
      assert {:reply, {:error, :invalid_consumer_group}, _} =
               Client.handle_call({:consumer_group_heartbeat, "", [member_epoch: 0]}, self(), state)
    end

    test "emits the consumer_group_heartbeat telemetry span", %{state: state} do
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

      stub(NetworkClient, :send_sync_request, fn _broker, wire, _timeout ->
        heartbeat_response(correlation_id(wire),
          member_id: "gen-id",
          member_epoch: 2,
          assignment: [{@known_id, [0, 1]}]
        )
      end)

      Client.handle_call({:consumer_group_heartbeat, "g", [member_epoch: 0, member_id: "gen-id"]}, self(), state)

      assert_received {[:kafka_ex, :consumer, :consumer_group_heartbeat, :start], start}
      assert %{group_id: "g", member_id: "gen-id", member_epoch: 0} = start

      assert_received {[:kafka_ex, :consumer, :consumer_group_heartbeat, :stop], stop}
      assert %{result: :ok, member_epoch: 0, new_member_epoch: 2, assigned_partitions: 2} = stop
    end
  end

  describe "consumer_group_describe" do
    test "an unknown group returns the group-level error", %{state: state} do
      stub(NetworkClient, :send_sync_request, fn _broker, wire, _timeout ->
        describe_response(correlation_id(wire), "g", 69)
      end)

      # same shape as DescribeGroups: {group_id, error}; KafkaEx.API unwraps it
      assert {:reply, {:error, {"g", :group_id_not_found}}, _} =
               Client.handle_call({:consumer_group_describe, ["g"], []}, self(), state)
    end

    test "parses a described group", %{state: state} do
      stub(NetworkClient, :send_sync_request, fn _broker, wire, _timeout ->
        describe_response(correlation_id(wire), "g", 0)
      end)

      assert {:reply, {:ok, [%ConsumerGroupDescription{group_id: "g", type: :consumer, members: []}]}, _} =
               Client.handle_call({:consumer_group_describe, ["g"], []}, self(), state)
    end

    test "a group-level :not_coordinator re-discovers the coordinator", %{state: state} do
      test_pid = self()

      stub(NetworkClient, :send_sync_request, fn _broker, wire, _timeout ->
        describe_response(correlation_id(wire), "g", 16)
      end)

      stub(RequestBuilder, :find_coordinator_request, fn _opts, _state ->
        send(test_pid, :rediscovered)
        {:error, :api_version_no_supported}
      end)

      Client.handle_call({:consumer_group_describe, ["g"], []}, self(), state)

      assert_received :rediscovered
    end
  end
end
