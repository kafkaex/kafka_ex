defmodule KafkaEx.Integration.Lifecycle.MetadataRefreshTest do
  @moduledoc """
  Incident regression: deleting a topic the client never used must not storm
  logs — only `tracked_topics` are refreshed, so an untouched topic never gates.
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
      # topics_metadata (unlike a bare metadata query) tracks topic_a.
      {:ok, _} = API.topics_metadata(client, [topic_a])

      _ = create_topic(client, topic_b)
      {:ok, _} = API.delete_topic(client, topic_b)

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

  # The used-topic-deletion warning is covered by metadata_missing_test.exs (unit),
  # not here: this shared cluster's follower brokers never receive the post-delete
  # metadata, so a live assertion is env-flaky, not a property of the code.

  describe "narrowing then recovery across metadata calls" do
    test "a tracked-only refresh narrows cluster_metadata, but topics_metadata recovers without loss", %{
      client: client
    } do
      topic1 = generate_random_string()
      topic2 = generate_random_string()
      topic3 = generate_random_string()

      _ = create_topic(client, topic1)
      _ = create_topic(client, topic2)
      _ = create_topic(client, topic3)

      {:ok, all_metadata} = API.metadata(client)
      assert Map.has_key?(all_metadata.topics, topic1)
      assert Map.has_key?(all_metadata.topics, topic2)
      assert Map.has_key?(all_metadata.topics, topic3)

      # API.metadata/1 overwrites cluster_metadata wholesale; it never touches tracked_topics.
      tracked_after_metadata = :sys.get_state(client).tracked_topics
      refute MapSet.member?(tracked_after_metadata, topic1)
      refute MapSet.member?(tracked_after_metadata, topic2)
      refute MapSet.member?(tracked_after_metadata, topic3)

      {:ok, _} = API.topics_metadata(client, [topic1])

      state_after_t1 = :sys.get_state(client)
      assert Map.has_key?(state_after_t1.cluster_metadata.topics, topic1)
      refute Map.has_key?(state_after_t1.cluster_metadata.topics, topic2)
      refute Map.has_key?(state_after_t1.cluster_metadata.topics, topic3)
      assert state_after_t1.tracked_topics == MapSet.new([topic1])

      {:ok, _} = API.topics_metadata(client, [topic2])

      state_after_t2 = :sys.get_state(client)
      assert Map.has_key?(state_after_t2.cluster_metadata.topics, topic1)
      assert Map.has_key?(state_after_t2.cluster_metadata.topics, topic2)
      assert state_after_t2.tracked_topics == MapSet.new([topic1, topic2])

      {:ok, _} = API.metadata(client)
      {:ok, _} = GenServer.call(client, :update_metadata, 15_000)

      # Monotonic tracked_topics: topic1/topic2 survive the tracked-only refresh, no thrashing.
      final_state = :sys.get_state(client)
      assert Map.has_key?(final_state.cluster_metadata.topics, topic1)
      assert Map.has_key?(final_state.cluster_metadata.topics, topic2)
    end
  end
end
