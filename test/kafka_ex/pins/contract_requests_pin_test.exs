defmodule KafkaEx.Pins.ContractRequestsPinTest do
  use KafkaEx.TestSupport.PinCase, async: false

  alias KafkaEx.Client.NodeSelector
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
