defmodule KafkaEx.Integration.Lifecycle.MetadataRefreshTest do
  @moduledoc """
  Regression for the production incident: an unrelated team deleted a topic the
  client never used, and the old whole-catalog metadata refresh stormed
  `:error` logs for hours. The fix tracks/refreshes only `tracked_topics`
  (topics the client actually uses), so a topic it never touches can never
  enter the missing-topic check, let alone log about it.
  """
  use ExUnit.Case, async: true
  @moduletag :lifecycle

  import ExUnit.CaptureLog
  import KafkaEx.TestHelpers
  import KafkaEx.IntegrationHelpers
  import KafkaEx.TestSupport.ProcessHelpers

  alias KafkaEx.API
  alias KafkaEx.Client

  setup do
    {:ok, args} = KafkaEx.build_worker_options([])
    {:ok, client} = Client.start_link(args, :no_name)
    on_exit(fn -> stop_safely(client) end)

    {:ok, %{client: client}}
  end

  describe "unrelated topic deletion" do
    test "deleting a topic the client never used logs nothing about it and A stays fresh", %{client: client} do
      topic_a = generate_random_string()
      topic_b = generate_random_string()

      _ = create_topic(client, topic_a)
      {:ok, _} = API.produce(client, topic_a, 0, [%{value: "seed"}])
      # topics_metadata unconditionally merges into tracked_topics (unlike bare
      # API.metadata/3, which is a one-off query) - this is what guarantees
      # topic_a is in the refresh set below, regardless of whether produce's
      # own node-selection happened to already know the broker.
      {:ok, _} = API.topics_metadata(client, [topic_a])

      # topic_b is created and deleted but the client never uses it, so it never
      # enters tracked_topics and the refresh set never includes it.
      _ = create_topic(client, topic_b)
      {:ok, _} = API.delete_topic(client, topic_b)

      # Pin down the actual mechanism under test, not just its absence of side effects.
      tracked = :sys.get_state(client).tracked_topics
      assert MapSet.member?(tracked, topic_a)
      refute MapSet.member?(tracked, topic_b)

      log =
        capture_log(fn ->
          for _ <- 1..5 do
            GenServer.call(client, :update_metadata, 15_000)
          end
        end)

      refute log =~ topic_b
      refute log =~ "[error]"
      refute log =~ "[warning]"

      {:ok, metadata} = API.metadata(client, [topic_a], [])
      topic = Map.get(metadata.topics, topic_a)
      assert topic != nil
      assert length(topic.partitions) >= 1
    end
  end

  # A second scenario (a topic the client DOES use gets deleted -> exactly one
  # edge-triggered warning naming it) was attempted here but dropped: on this
  # sandbox's shared docker cluster, follower brokers were observed to never
  # receive the controller's post-delete UpdateMetadataRequest (confirmed by
  # querying each broker directly - the leader broker correctly reported the
  # topic gone while followers kept reporting it present, indefinitely, well
  # past any reasonable propagation window). `NodeSelector.first_available/0`
  # can land on such a stale follower, making the assertion's timing
  # fundamentally unreliable in this environment rather than a property of the
  # code under test. See task report for the full investigation.
end
