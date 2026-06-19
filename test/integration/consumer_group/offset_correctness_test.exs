defmodule KafkaEx.Integration.ConsumerGroup.OffsetCorrectnessTest do
  @moduledoc """
  Integration regression for PR-4 / defect H-6: on consumer startup, `load_offsets`
  must NOT silently fall back to `auto_offset_reset` when the committed-offset fetch
  keeps failing with a real error — that loses/duplicates the committed position. It
  must retry retryable errors and then raise (propagate -> supervisor restart).

  We force a real NOT_COORDINATOR deterministically (no broker killing) by seeding the
  client's coordinator cache with the wrong broker via `:sys.replace_state`, then run
  the consumer's startup offset-load path (`handle_info(:timeout, ...)`) against the
  live cluster.

  The complementary positive path (commit -> restart -> resume from the committed
  offset, no reprocessing) is covered by SyncConsumerGroupTest.
  """
  use ExUnit.Case, async: false
  @moduletag :consumer_group

  import KafkaEx.TestHelpers
  import KafkaEx.IntegrationHelpers

  alias KafkaEx.API
  alias KafkaEx.Client
  alias KafkaEx.Client.State, as: ClientState
  alias KafkaEx.Consumer.GenConsumer
  alias KafkaEx.Consumer.GenConsumer.State

  setup do
    {:ok, args} = KafkaEx.build_worker_options([])
    {:ok, client} = Client.start_link(args, :no_name)
    on_exit(fn -> if Process.alive?(client), do: GenServer.stop(client) end)

    original = Application.get_env(:kafka_ex, :load_offsets_retry_backoff_ms)
    Application.put_env(:kafka_ex, :load_offsets_retry_backoff_ms, 0)

    on_exit(fn ->
      if is_nil(original),
        do: Application.delete_env(:kafka_ex, :load_offsets_retry_backoff_ms),
        else: Application.put_env(:kafka_ex, :load_offsets_retry_backoff_ms, original)
    end)

    {:ok, %{client: client}}
  end

  @tag timeout: 60_000
  test "load_offsets raises instead of silently resetting when the committed-offset fetch keeps failing",
       %{client: client} do
    topic = generate_random_string()
    group = generate_random_string()
    _ = create_topic(client, topic, partitions: 1)
    wait_for_topic_in_metadata(client, topic)

    partitions = [%{partition_num: 0}]

    # establish the real coordinator and read the broker set
    assert {:ok, _} = API.fetch_committed_offset(client, group, topic, partitions)
    cstate = :sys.get_state(client)
    real_coord = cstate.cluster_metadata.consumer_group_coordinators[group]
    wrong_node = cstate.cluster_metadata.brokers |> Map.keys() |> Enum.find(&(&1 != real_coord))

    assert wrong_node, "need a multi-broker cluster to pick a non-coordinator broker"

    # seed the WRONG coordinator -> the startup offset fetch hits a broker that really
    # is not the coordinator for this group and keeps returning NOT_COORDINATOR
    :sys.replace_state(client, fn st -> ClientState.put_consumer_group_coordinator(st, group, wrong_node) end)

    state = %State{
      current_offset: nil,
      last_commit: nil,
      client: client,
      group: group,
      topic: topic,
      partition: 0,
      auto_offset_reset: :latest,
      api_versions: %{}
    }

    assert_raise RuntimeError, ~r/Unable to load committed offsets/, fn ->
      GenConsumer.handle_info(:timeout, state)
    end
  end
end
