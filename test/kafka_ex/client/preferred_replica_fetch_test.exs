defmodule KafkaEx.Client.PreferredReplicaFetchTest do
  @moduledoc false
  use KafkaEx.TestSupport.PinCase, async: false

  alias KafkaEx.Messages.Fetch

  @leader 1
  @replica 2
  @offset_out_of_range 1

  setup do
    {:ok, start_pinned(topics: %{@topic => [@leader]}, api_versions: %{fetch: 0..11})}
  end

  @tag pin_set: :invariant
  test "an error from the preferred replica is retried once at the leader", %{fake: fake, client: client} do
    mark = with_preferred_replica(fake, client, @replica)
    reply_by_node(fake, %{@replica => @offset_out_of_range, @leader => 0})

    {result, _log} = with_log(fn -> fetch(client) end)

    assert {:ok, %Fetch{}} = result
    assert fetched_from(fake, mark) == [@replica, @leader]
    assert_preferred_replica_forgotten(fake, client)
  end

  @tag pin_set: :invariant
  test "the leader's own error is returned after the fallback", %{fake: fake, client: client} do
    mark = with_preferred_replica(fake, client, @replica)
    reply_by_node(fake, %{@replica => @offset_out_of_range, @leader => @offset_out_of_range})

    {result, _log} = with_log(fn -> fetch(client) end)

    assert {:error, %{error: :offset_out_of_range}} = result
    assert fetched_from(fake, mark) == [@replica, @leader]
  end

  @tag pin_set: :invariant
  test "the replica's error uses up no attempt, so the leader gets all of them", %{fake: fake, client: client} do
    mark = with_preferred_replica(fake, client, @replica)
    reply_by_node(fake, %{@replica => @offset_out_of_range, @leader => :close})

    {result, _log} = with_log(fn -> fetch(client) end)

    assert {:error, %{error: :closed}} = result
    assert fetched_from(fake, mark) == [@replica | List.duplicate(@leader, Client.retry_count())]
  end

  @tag pin_set: :invariant
  test "a leader error without a preferred replica is not sent twice", %{fake: fake, client: client} do
    reply_by_node(fake, %{@leader => @offset_out_of_range})

    {result, _log} = with_log(fn -> fetch(client) end)

    assert {:error, %{error: :offset_out_of_range}} = result
    assert fetched_from(fake, 0) == [@leader]
  end

  defp fetch(client), do: Client.request(client, {:fetch, @topic, 0, 5, [api_version: 0]}, 5_000)

  # The leader names the replica in a v11 answer; the client caches it for the next fetch.
  defp with_preferred_replica(fake, client, node_id) do
    FakeTransport.respond(fake, :fetch, &{:frame, fetch_frame(&1, node_id)})
    {:ok, _} = Client.request(client, {:fetch, @topic, 0, 5, [api_version: 11]}, 5_000)
    count(fake, :fetch)
  end

  defp reply_by_node(fake, reply_by_node) do
    FakeTransport.respond(fake, :fetch, fn %{node_id: node_id, correlation_id: correlation_id} ->
      case Map.fetch!(reply_by_node, node_id) do
        :close -> :close
        error_code -> {:frame, fetch_v0_response(correlation_id, error_code)}
      end
    end)
  end

  defp fetched_from(fake, mark), do: fake |> nodes(:fetch) |> Enum.drop(mark)

  defp assert_preferred_replica_forgotten(fake, client) do
    reply_by_node(fake, %{@leader => 0})
    mark = count(fake, :fetch)

    {result, _log} = with_log(fn -> fetch(client) end)

    assert {:ok, %Fetch{}} = result
    assert [@leader | _] = fetched_from(fake, mark)
  end

  defp fetch_v0_response(correlation_id, error_code) do
    <<correlation_id::32-signed, 1::32-signed, byte_size(@topic)::16-signed>> <>
      @topic <> <<1::32-signed, 0::32-signed, error_code::16-signed, 10::64-signed, 0::32-signed>>
  end
end
