defmodule KafkaEx.Pins.ContractRequestsPinTest do
  use KafkaEx.TestSupport.PinCase, async: false

  alias KafkaEx.Client.Error
  alias KafkaEx.Client.NodeSelector
  alias KafkaEx.Messages.ApiVersions
  alias KafkaEx.Messages.JoinGroup
  alias KafkaEx.Messages.RecordMetadata
  alias KafkaEx.TestSupport.FakeTransport.Wire

  @moduletag pin_set: :invariant

  @not_leader_for_partition 6
  @member_id_required 79

  setup do
    start_pinned()
  end

  defp produce_request do
    record_set = %Kayrock.RecordBatch{records: [%Kayrock.RecordBatch.Record{value: "v"}]}

    %Kayrock.Produce.V3.Request{
      acks: 1,
      timeout: 1000,
      topic_data: [%{topic: @topic, data: [%{partition: 0, record_set: record_set}]}]
    }
  end

  defp join(client, member_id),
    do: API.join_group(client, "g", member_id, topics: [@topic], session_timeout: 10_000, rebalance_timeout: 10_000)

  # Every connection opens with its own ApiVersions handshake; only a later one on it is the caller's request.
  defp after_handshake(fake, versions, reply) do
    handshaken = :ets.new(:handshaken, [:public])
    for %{conn: conn} <- requests(fake, :api_versions), do: :ets.insert(handshaken, {conn})
    fn request -> if :ets.insert_new(handshaken, {request.conn}), do: versions, else: reply.(request) end
  end

  describe "send_request/4" do
    test "is written once and returns the transport error when the peer closes", %{fake: fake, client: client} do
      FakeTransport.respond(fake, :produce, :close)

      {result, _log} =
        with_log(fn -> Client.send_request(client, produce_request(), NodeSelector.topic_partition(@topic, 0)) end)

      assert count(fake, :produce) == 1
      assert Enum.uniq(nodes(fake, :produce)) == [1]
      assert result == {:error, :closed}
    end

    test "with :first_available tries each candidate once, then returns :broker_failed", %{
      fake: fake,
      client: client
    } do
      {:ok, %{brokers: brokers}} = API.cluster_metadata(client)
      booted = nodes(fake, :metadata)
      FakeTransport.respond(fake, :metadata, :close)

      {result, _log} =
        with_log(fn ->
          Client.send_request(client, %Kayrock.Metadata.V0.Request{topics: []}, NodeSelector.first_available())
        end)

      tried = fake |> nodes(:metadata) |> Enum.drop(length(booted))
      assert result == {:error, :broker_failed}
      assert tried != []
      assert Enum.sort(tried) == brokers |> Map.keys() |> Enum.sort()
      assert tried == Enum.uniq(tried)
    end
  end

  describe "metadata/2 and api_versions/2 walk the brokers" do
    test "metadata/2 answers from the next broker when the first closes", %{fake: fake, client: client, host: host} do
      closed = :atomics.new(1, [])

      FakeTransport.respond(fake, :metadata, fn _request ->
        if :atomics.exchange(closed, 1, 1) == 0, do: :close, else: metadata_reply(host, %{@topic => [1, 2]})
      end)

      mark = count(fake, :metadata)

      {result, log} = with_log(fn -> API.metadata(client) end)

      assert {:ok, %{brokers: %{1 => _, 2 => _}}} = result
      assert log =~ ~r/Network call to broker \d .* failed: :closed\n/
      assert fake |> nodes(:metadata) |> Enum.drop(mark) |> Enum.sort() == [1, 2]
    end

    test "api_versions/2 answers from the next broker when the first closes", %{fake: fake, client: client} do
      closed = :atomics.new(1, [])
      versions = Wire.api_versions_fields(:sys.get_state(fake).versions)

      FakeTransport.respond(
        fake,
        :api_versions,
        after_handshake(fake, versions, fn _request ->
          if :atomics.exchange(closed, 1, 1) == 0, do: :close, else: versions
        end)
      )

      {result, log} = with_log(fn -> API.api_versions(client) end)

      assert {:ok, %ApiVersions{}} = result
      assert log =~ ~r/Network call to broker \d .* failed: :closed\n/
    end

    test "every broker answering an unparsable frame ends on :broker_failed, one write per broker per attempt", %{
      fake: fake,
      client: client
    } do
      unparsable = &{:frame, <<&1.correlation_id::32, 0xFF>>}
      versions = Wire.api_versions_fields(:sys.get_state(fake).versions)
      FakeTransport.respond(fake, :metadata, unparsable)
      FakeTransport.respond(fake, :api_versions, after_handshake(fake, versions, unparsable))
      mark = count(fake, :metadata)

      {{metadata, api_versions}, _log} = with_log(fn -> {API.metadata(client), API.api_versions(client)} end)

      assert {:error, %Error{error: :broker_failed}} = metadata
      assert api_versions == {:error, :broker_failed}
      attempts = Client.retry_count()
      assert fake |> nodes(:metadata) |> Enum.drop(mark) |> Enum.frequencies() == %{1 => attempts, 2 => attempts}
    end

    test "a walk in which every broker timed out ends on :timeout", %{fake: fake, client: client} do
      with_request_timeout(200)
      FakeTransport.respond(fake, :metadata, :no_reply)

      {result, _log} = with_log(fn -> API.metadata(client) end)

      assert {:error, %Error{error: :timeout}} = result
    end

    test "a walk in which one broker timed out and another closed ends on :broker_failed", %{
      fake: fake,
      client: client
    } do
      with_request_timeout(200)
      FakeTransport.respond(fake, :metadata, fn request -> if request.node_id == 1, do: :no_reply, else: :close end)

      {result, _log} = with_log(fn -> API.metadata(client) end)

      assert {:error, %Error{error: :broker_failed}} = result
    end

    test "find_coordinator/3 walks every broker on each attempt", %{fake: fake, client: client} do
      FakeTransport.respond(fake, :find_coordinator, :close)

      {result, _log} = with_log(fn -> API.find_coordinator(client, "g") end)

      attempts = Client.retry_count()
      assert result == {:error, :broker_failed}
      assert fake |> nodes(:find_coordinator) |> Enum.frequencies() == %{1 => attempts, 2 => attempts}
    end

    test "a walk every broker failed is retried: a single broker that closes once answers the next walk" do
      single = fn request -> Wire.metadata_fields([{1, request.host, 9092}], %{@topic => [1]}) end
      %{fake: fake, client: client} = start_pinned(before_boot: &FakeTransport.respond(&1, :metadata, single))
      closed = :atomics.new(1, [])

      FakeTransport.respond(fake, :metadata, fn request ->
        if :atomics.exchange(closed, 1, 1) == 0, do: :close, else: single.(request)
      end)

      mark = count(fake, :metadata)

      {result, _log} = with_log(fn -> API.metadata(client) end)

      assert {:ok, %{brokers: brokers}} = result
      assert Map.keys(brokers) == [1]
      assert fake |> nodes(:metadata) |> Enum.drop(mark) == [1, 1]
    end
  end

  describe "the JoinGroup / SyncGroup cap" do
    test "JoinGroup is written once when the peer closes", %{fake: fake, client: client} do
      FakeTransport.respond(fake, :join_group, :close)

      {result, _log} = with_log(fn -> join(client, "m") end)

      assert result == {:error, :closed}
      assert count(fake, :join_group) == 1
      assert Enum.uniq(nodes(fake, :join_group)) == [2]
    end

    test "SyncGroup is written once when the peer closes", %{fake: fake, client: client} do
      FakeTransport.respond(fake, :sync_group, :close)

      {result, _log} = with_log(fn -> API.sync_group(client, "g", 1, "m") end)

      assert result == {:error, :closed}
      assert count(fake, :sync_group) == 1
      assert Enum.uniq(nodes(fake, :sync_group)) == [2]
    end

    test "the KIP-394 member_id_required re-issue is part of the join, not a second attempt", %{
      fake: fake,
      client: client
    } do
      joins = :counters.new(1, [])

      FakeTransport.respond(fake, :join_group, fn _request ->
        :counters.add(joins, 1, 1)

        if :counters.get(joins, 1) == 1,
          do: %{error_code: @member_id_required, member_id: "assigned", protocol_name: "", leader: "", members: []},
          else: %{generation_id: 1, member_id: "assigned", protocol_name: "roundrobin", leader: "assigned", members: []}
      end)

      {result, _log} = with_log(fn -> join(client, "") end)

      assert {:ok, %JoinGroup{member_id: "assigned", generation_id: 1}} = result
      assert count(fake, :join_group) == 2
      assert Enum.uniq(nodes(fake, :join_group)) == [2]
    end
  end

  describe "retry predicates and error atoms" do
    test "produce retries a leadership error and then returns it", %{fake: fake, client: client} do
      FakeTransport.respond(fake, :produce, %{
        responses: [
          %{
            topic: @topic,
            partition_responses: [%{partition: 0, error_code: @not_leader_for_partition, base_offset: -1}]
          }
        ]
      })

      {result, _log} = with_log(fn -> API.produce(client, @topic, 0, [%{value: "v"}]) end)

      assert result == {:error, :not_leader_for_partition}
      assert count(fake, :produce) == 3
      assert Enum.uniq(nodes(fake, :produce)) == [1]
    end

    test "a retried fetch waits a growing backoff between its attempts", %{fake: fake, client: client} do
      test_pid = self()

      FakeTransport.respond(fake, :fetch, fn _request ->
        send(test_pid, {:fetch_sent, System.monotonic_time(:millisecond)})
        :close
      end)

      {result, _log} = with_log(fn -> API.fetch(client, @topic, 0, 0, max_wait_time: 10) end)

      assert result == {:error, :closed}
      assert [first, second, third] = for(_ <- 1..3, do: receive(do: ({:fetch_sent, at} -> at)))
      assert second - first >= 80
      assert third - second >= 160
    end

    test "a request with no answer within its budget returns :timeout", %{fake: fake, client: client} do
      with_request_timeout(200)
      FakeTransport.respond(fake, :produce, :no_reply)

      {result, _log} = with_log(fn -> API.produce(client, @topic, 0, [%{value: "v"}]) end)

      assert result == {:error, :timeout}
    end

    test "a request whose target cannot be resolved returns :no_broker", %{client: client} do
      # Keeps a request that a future client may queue below ExUnit's timeout.
      with_request_timeout(2_000)
      {result, _log} = with_log(fn -> API.produce(client, "no-such-topic", 0, [%{value: "v"}]) end)

      assert result == {:error, :no_broker}
    end

    test "a non-atom transport reason is tagged :transport_error, an atom passes through", %{
      fake: fake,
      client: client
    } do
      # Keeps a request that a future client may queue below ExUnit's timeout.
      with_request_timeout(2_000)
      FakeTransport.fail_send(fake, :produce, 3, {:tls_alert, :bad_record_mac})
      {tagged, _log} = with_log(fn -> API.produce(client, @topic, 0, [%{value: "v"}]) end)
      FakeTransport.release(fake, :produce)
      FakeTransport.fail_send(fake, :produce, 3, :econnreset)
      {plain, _log} = with_log(fn -> API.produce(client, @topic, 0, [%{value: "v"}]) end)

      assert tagged == {:error, :transport_error}
      assert plain == {:error, :econnreset}
    end
  end

  describe "acks 0" do
    test "completes on write, is written once and carries no offset", %{fake: fake, client: client} do
      FakeTransport.respond(fake, :produce, produce_reply())

      assert {:ok, %RecordMetadata{base_offset: nil}} = API.produce(client, @topic, 0, [%{value: "v"}], acks: 0)
      # One connection answers in arrival order, so this reply proves every earlier write was read.
      assert {:ok, %RecordMetadata{base_offset: 7}} = API.produce(client, @topic, 0, [%{value: "v"}], acks: 1)

      produces = requests(fake, :produce)
      assert Enum.map(produces, &Wire.acks0?(&1.frame)) == [true, false]
      assert produces |> Enum.map(& &1.conn) |> Enum.uniq() |> length() == 1
      assert Enum.uniq(nodes(fake, :produce)) == [1]
    end
  end
end
