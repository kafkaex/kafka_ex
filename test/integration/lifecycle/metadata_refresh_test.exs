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
end
