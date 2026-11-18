defmodule KafkaEx.Pins.SelectionPinTest do
  use KafkaEx.TestSupport.PinCase, async: false

  @moduletag pin_set: :invariant

  @not_leader_for_partition 6
  @coordinator_not_available 15
  @not_coordinator 16

  setup do
    start_pinned()
  end

  defp produce(client, partition), do: API.produce(client, @topic, partition, [%{value: "v"}])

  defp consumer_group_heartbeat(client), do: API.consumer_group_heartbeat(client, "g", member_id: "m", member_epoch: 0)

  defp refresh(client), do: GenServer.call(client, :update_metadata)

  defp metadata_topics(%{frame: frame}) do
    <<_api::16, _version::16, _corr::32, client_id_size::16-signed, _client_id::binary-size(client_id_size),
      count::32-signed, rest::binary>> = frame

    if count < 0, do: :all, else: strings(rest, count)
  end

  defp strings(_rest, 0), do: []
  defp strings(<<size::16-signed, value::binary-size(size), rest::binary>>, n), do: [value | strings(rest, n - 1)]

  defp new_metadata_requests(fake, before), do: fake |> requests(:metadata) |> Enum.drop(before)

  defp sequence(replies) do
    counter = :counters.new(1, [])

    fn _request ->
      :counters.add(counter, 1, 1)
      Enum.at(replies, min(:counters.get(counter, 1), length(replies)) - 1)
    end
  end

  defp coordinator_reply(host, node_id, error_code),
    do: %{error_code: error_code, node_id: node_id, host: host, port: 9093, error_message: nil}

  describe "a failed selection" do
    test "refreshes the metadata once, then returns :no_broker without a write", %{fake: fake, client: client} do
      before = count(fake, :metadata)

      {result, _log} = with_log(fn -> produce(client, 9) end)

      assert result == {:error, :no_broker}
      assert length(new_metadata_requests(fake, before)) == 1
      assert count(fake, :produce) == 0
    end

    test "refreshes once per failed attempt of a retried request", %{fake: fake, client: client} do
      before = count(fake, :metadata)

      {result, _log} = with_log(fn -> API.fetch(client, @topic, 9, 0, max_wait_time: 10) end)

      assert result == {:error, :no_broker}
      assert length(new_metadata_requests(fake, before)) == Client.retry_count()
      assert count(fake, :fetch) == 0
    end

    test "adds its topic to the topics every later refresh asks for", %{fake: fake, client: client} do
      {:ok, _} = refresh(client)
      [untracked] = fake |> requests(:metadata) |> Enum.take(-1) |> Enum.map(&metadata_topics/1)

      {_result, _log} = with_log(fn -> produce(client, 9) end)
      {:ok, _} = refresh(client)

      assert untracked == []
      assert fake |> requests(:metadata) |> List.last() |> metadata_topics() == [@topic]
    end

    test "of a coordinator re-discovers it without forgetting the cached one", %{fake: fake, client: client} do
      FakeTransport.respond(
        fake,
        :find_coordinator,
        sequence([coordinator_reply("gone", 7, 0), coordinator_reply("gone", 7, @coordinator_not_available)])
      )

      {result, log} = with_log([level: :debug], fn -> API.heartbeat(client, "g", "m", 1) end)
      {:ok, metadata} = API.cluster_metadata(client)

      assert result == {:error, :no_broker}
      assert count(fake, :find_coordinator) == Client.retry_count()
      assert metadata.consumer_group_coordinators == %{"g" => 7}
      assert log =~ "No broker for consumer group g after metadata refresh: :broker_not_found"
    end

    test "logs the reason at debug level", %{client: client} do
      debug = capture_log([level: :debug], fn -> produce(client, 9) end)
      info = capture_log([level: :info], fn -> produce(client, 9) end)

      assert debug =~ "No broker for #{@topic}/9 after metadata refresh: :no_such_partition"
      refute info =~ "No broker for"
    end
  end

  describe "a leadership error" do
    setup %{fake: fake} do
      FakeTransport.respond(fake, :produce, %{
        responses: [
          %{
            topic: @topic,
            partition_responses: [%{partition: 0, error_code: @not_leader_for_partition, base_offset: -1}]
          }
        ]
      })
    end

    test "refreshes the tracked topics, not the failing request's topic", %{fake: fake, client: client} do
      before = count(fake, :metadata)

      {result, _log} = with_log(fn -> produce(client, 0) end)

      assert result == {:error, :not_leader_for_partition}

      assert fake |> new_metadata_requests(before) |> Enum.map(&metadata_topics/1) ==
               List.duplicate([], Client.retry_count())
    end

    test "logs the refresh at info level", %{client: client} do
      info = capture_log([level: :info], fn -> produce(client, 0) end)
      warning = capture_log([level: :warning], fn -> produce(client, 0) end)

      assert info =~ "Refreshing metadata due to :not_leader_for_partition error"
      refute warning =~ "Refreshing metadata"
    end
  end

  describe "a coordinator error" do
    test "drops the coordinator before re-discovering it, so the retry uses the new one", %{
      fake: fake,
      client: client,
      host: host
    } do
      FakeTransport.respond(fake, :heartbeat, sequence([%{error_code: @not_coordinator}, %{error_code: 0}]))

      FakeTransport.respond(
        fake,
        :find_coordinator,
        sequence([coordinator_reply(host, 2, 0), coordinator_reply(host, 1, 0)])
      )

      {first, log} = with_log([level: :info], fn -> API.heartbeat(client, "g", "m", 1) end)
      retried_on_new = count(fake, :heartbeat, 1)
      second = API.heartbeat(client, "g", "m", 1)

      assert first == {:ok, :no_error}
      assert second == {:ok, :no_error}
      assert retried_on_new == 1
      assert count(fake, :find_coordinator) == 2
      assert count(fake, :heartbeat) == 3
      assert log =~ "Re-discovering group coordinator due to :not_coordinator error"
    end

    test "logs the re-discovery at info level", %{fake: fake, client: client} do
      FakeTransport.respond(fake, :heartbeat, %{error_code: @not_coordinator})

      info = capture_log([level: :info], fn -> API.heartbeat(client, "g", "m", 1) end)
      warning = capture_log([level: :warning], fn -> API.heartbeat(client, "g", "m", 1) end)

      assert info =~ "Re-discovering group coordinator due to :not_coordinator error"
      refute warning =~ "Re-discovering group coordinator"
    end

    test "leaves no coordinator cached when the re-discovery fails", %{fake: fake, client: client, host: host} do
      FakeTransport.respond(fake, :heartbeat, %{error_code: @not_coordinator})

      FakeTransport.respond(
        fake,
        :find_coordinator,
        sequence([coordinator_reply(host, 2, 0), coordinator_reply(host, 2, @coordinator_not_available)])
      )

      {result, _log} = with_log(fn -> API.heartbeat(client, "g", "m", 1) end)
      {:ok, metadata} = API.cluster_metadata(client)

      assert result == {:error, :no_broker}
      assert metadata.consumer_group_coordinators == %{}
    end
  end

  describe "a connection error on a request that re-discovers its coordinator" do
    test "forgets the coordinator and logs it at info level", %{fake: fake, client: client} do
      FakeTransport.respond(fake, :consumer_group_heartbeat, :close)

      {result, log} =
        with_log([level: :info], fn -> consumer_group_heartbeat(client) end)

      assert result == {:error, :closed}
      assert count(fake, :find_coordinator) == Client.retry_count()
      assert log =~ "Forgetting the coordinator of g after :closed"
    end

    test "logs the forgetting at info level", %{fake: fake, client: client} do
      FakeTransport.respond(fake, :consumer_group_heartbeat, :close)

      info = capture_log([level: :info], fn -> consumer_group_heartbeat(client) end)
      warning = capture_log([level: :warning], fn -> consumer_group_heartbeat(client) end)

      assert info =~ "Forgetting the coordinator of g after :closed"
      refute warning =~ "Forgetting the coordinator"
    end
  end
end
