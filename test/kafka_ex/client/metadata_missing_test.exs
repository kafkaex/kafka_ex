defmodule KafkaEx.Client.MetadataMissingTest do
  @moduledoc """
  Metadata give-up path: a tracked topic still missing after retries must not
  discard the whole refresh (liveness), and its warning must be edge-triggered.
  Driven through a started client on the fake transport.
  """
  use KafkaEx.TestSupport.PinCase, async: false

  alias KafkaEx.Cluster.ClusterMetadata

  @moduletag pin_set: :invariant

  setup do
    {:ok,
     start_pinned(
       topics: %{"present-topic" => [1], "missing-topic" => [1]},
       client: [initial_topics: ["present-topic", "missing-topic"]]
     )}
  end

  defp answer_with(%{fake: fake, host: host}, topic_names),
    do: FakeTransport.respond(fake, :metadata, metadata_reply(host, Map.new(topic_names, &{&1, [1]})))

  defp refresh(client), do: with_log(fn -> refresh_tracked(client) end)

  # A repeat refresh that is one request and logs no warning means the known-missing set is unchanged.
  defp assert_known_missing(fake, client) do
    before = count(fake, :metadata)
    sent_before = length(non_handshake(fake))
    {_, log} = refresh(client)
    assert count(fake, :metadata) - before == 1
    assert length(non_handshake(fake)) - sent_before == 1
    refute log =~ "[warning]"
  end

  test "give-up merges: missing topic doesn't discard metadata for topics that ARE present",
       %{
         fake: fake,
         client: client
       } = pinned do
    answer_with(pinned, ["present-topic"])

    {{:ok, cluster_metadata}, _log} = refresh(client)

    assert %ClusterMetadata{} = cluster_metadata
    assert cluster_metadata.topics["present-topic"]
    refute cluster_metadata.topics["missing-topic"]

    # Old give-up returned {state, nil}, discarding present-topic's fresh metadata.
    {:ok, cached} = API.cluster_metadata(client)
    assert cached.topics["present-topic"]
    assert_known_missing(fake, client)
  end

  test "warning logged once when a tracked topic first goes missing", %{client: client} = pinned do
    answer_with(pinned, ["present-topic"])

    {_, log} = refresh(client)

    assert log =~ "[warning]"
    assert log =~ "missing-topic"
    assert log =~ "unavailable"
  end

  test "edge-triggered: repeating the same missing set within the heartbeat window logs no second warning",
       %{
         fake: fake,
         client: client
       } = pinned do
    answer_with(pinned, ["present-topic"])

    {_, first_log} = refresh(client)

    assert first_log =~ "[warning]"

    {_, second_log} = refresh(client)

    refute second_log =~ "[warning]"
    assert_known_missing(fake, client)
  end

  test "recovery: topic reappearing logs exactly one info line and no warnings",
       %{fake: fake, client: client} = pinned do
    answer_with(pinned, ["present-topic"])
    refresh(client)

    answer_with(pinned, ["present-topic", "missing-topic"])

    {{:ok, cluster_metadata}, recovery_log} = refresh(client)

    assert cluster_metadata.topics["present-topic"]
    assert cluster_metadata.topics["missing-topic"]

    refute recovery_log =~ "[warning]"
    assert recovery_log =~ "[info]"
    assert recovery_log =~ "recovered"

    # The known-missing set was cleared: the topic going missing again warns afresh.
    answer_with(pinned, ["present-topic"])
    before_again = count(fake, :metadata)
    {_, again_log} = refresh(client)
    assert again_log =~ "[warning]"
    assert count(fake, :metadata) - before_again > 1
  end

  test "a changed missing set logs a fresh warning even within the heartbeat window",
       %{
         fake: fake,
         client: client
       } = pinned do
    answer_with(pinned, ["present-topic"])

    {_, first_log} = refresh(client)

    assert first_log =~ "[warning]"
    assert_known_missing(fake, client)

    # Both tracked topics now missing: the set changed, so a new warning must fire
    # despite still being inside the heartbeat window.
    answer_with(pinned, [])

    {_, second_log} = refresh(client)

    assert second_log =~ "[warning]"
    assert second_log =~ "present-topic"
    assert_known_missing(fake, client)
  end

  test "a known-missing topic is not sleep-retried on later refreshes (no recurring 600ms stall)",
       %{
         fake: fake,
         client: client
       } = pinned do
    answer_with(pinned, ["present-topic"])
    before_first = count(fake, :metadata)

    refresh(client)

    # First time a topic goes missing we still retry, to tolerate a transient gap.
    assert count(fake, :metadata) - before_first > 1

    before_second = count(fake, :metadata)
    sent_before_second = length(non_handshake(fake))

    refresh(client)

    # Already known-missing: a single metadata request, no retry ladder, no sleep.
    assert count(fake, :metadata) - before_second == 1
    assert length(non_handshake(fake)) - sent_before_second == 1
  end
end
