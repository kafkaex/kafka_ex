defmodule KafkaEx.Integration.ConsumerGroup.OffsetCorrectnessTest do
  @moduledoc """
  Integration regression for PR-4 / defect H-6: on consumer startup, `load_offsets`
  must resume from the *committed* offset and must NOT fall back to `auto_offset_reset`
  when a real committed offset exists — silently resetting would replay or skip
  messages (offset duplication/loss) on a real cluster.

  We commit a known offset, then drive the consumer's startup offset-load path
  (`handle_info(:timeout, ...)`) against the live cluster and assert it resumes from
  exactly that offset — not from `auto_offset_reset: :latest` (which would be the
  log-end), proving the committed position wins.

  The negative path (load_offsets *raising* instead of silently resetting when the
  committed-offset fetch keeps failing) is covered deterministically by the unit test
  `KafkaEx.Consumer.GenConsumerLoadOffsetsTest`: it cannot be reproduced as an
  integration test, because a healthy cluster self-heals a stale coordinator
  (`:not_coordinator` -> re-discover, see CoordinatorRediscoveryTest / #547), so a
  *persistent* real error is not deterministically forceable here.
  """
  use ExUnit.Case, async: false
  @moduletag :consumer_group

  import KafkaEx.TestHelpers
  import KafkaEx.IntegrationHelpers
  import KafkaEx.TestSupport.ProcessHelpers

  alias KafkaEx.API
  alias KafkaEx.Client
  alias KafkaEx.Consumer.GenConsumer
  alias KafkaEx.Consumer.GenConsumer.State

  setup do
    {:ok, args} = KafkaEx.build_worker_options([])
    {:ok, client} = Client.start_link(args, :no_name)
    on_exit(fn -> stop_safely(client) end)

    {:ok, %{client: client}}
  end

  @committed_offset 3

  @tag timeout: 60_000
  test "load_offsets resumes from the committed offset instead of resetting to auto_offset_reset",
       %{client: client} do
    topic = generate_random_string()
    group = generate_random_string()
    _ = create_topic(client, topic, partitions: 1)
    wait_for_topic_in_metadata(client, topic)

    # produce 5 records (offsets 0..4) so the log-end is 5 -> distinct from the offset
    # we commit (3); a wrong :latest reset would land on 5, a resume lands on 3.
    Enum.each(1..5, fn i -> {:ok, _} = API.produce(client, topic, 0, [%{value: "m-#{i}"}]) end)

    partitions = [%{partition_num: 0, offset: @committed_offset}]
    assert {:ok, _} = API.commit_offset(client, group, topic, partitions)

    # sanity: the commit is durable and readable from the coordinator
    assert {:ok, [%{partition_offsets: [%{offset: @committed_offset, error_code: :no_error}]}]} =
             API.fetch_committed_offset(client, group, topic, [%{partition_num: 0}])

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

    {:noreply, new_state, 0} = GenConsumer.handle_info(:timeout, state)

    assert new_state.current_offset == @committed_offset,
           "expected resume from committed offset #{@committed_offset}, got #{inspect(new_state.current_offset)}"
  end
end
