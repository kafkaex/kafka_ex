defmodule KafkaEx.Consumer.GenConsumerControlBatchTest do
  @moduledoc """
  Regression for the control/aborted-batch consume path.

  When a fetch returns batches whose records are all filtered out (transaction
  control markers / aborted records), `records` is empty but the consumer must
  still advance past them — otherwise the next fetch lands on the same batch and
  the consumer spins on one offset forever (cf. KafkaJS #403). The broker's raw
  batch metadata drives the advance (`Fetch.next_offset`), exactly as brod and
  the Java client do. A genuinely empty fetch (no batches → caught up) must NOT
  advance.

  Exercised through the real consume loop with a scripted `MockClient`.
  """
  use ExUnit.Case, async: false

  import KafkaEx.TestSupport.ProcessHelpers

  alias KafkaEx.Consumer.GenConsumer
  alias KafkaEx.Messages.Fetch
  alias KafkaEx.Test.MockClient

  defmodule TestConsumer do
    use KafkaEx.Consumer.GenConsumer
    def handle_message_set(_messages, state), do: {:async_commit, state}
  end

  defp fetch_offsets(calls) do
    for {:fetch, _topic, _partition, offset, _opts} <- calls, do: offset
  end

  # Starts a consumer at offset 0 against a MockClient that returns `fetch_response`
  # for every fetch, lets it run a few consume cycles, then returns the recorded calls.
  defp run(fetch_response) do
    {:ok, client} =
      MockClient.start_link(%{
        offset_fetch: {:ok, [%{partition_offsets: [%{offset: 0, error_code: :no_error}]}]},
        fetch: fetch_response,
        offset_commit: {:ok, []}
      })

    on_exit(fn -> stop_safely(client) end)

    {:ok, pid} =
      GenServer.start_link(GenConsumer, {TestConsumer, "g", "t", 0, [client: client]})

    calls = MockClient.wait_for_calls(client, 4)
    Process.unlink(pid)
    stop_safely(pid)

    calls
  end

  test "advances past a control-only fetch instead of re-fetching the same offset forever" do
    # Records filtered to [], but the raw batches covered up to offset 5, so
    # next_offset is 6. The consumer must move from 0 to 6.
    fetch =
      {:ok,
       %Fetch{
         topic: "t",
         partition: 0,
         records: [],
         last_offset: nil,
         next_offset: 6,
         high_watermark: 100
       }}

    offsets = fetch_offsets(run(fetch))

    assert 0 in offsets, "expected the initial fetch at offset 0"
    assert 6 in offsets, "consumer must advance past the control batch to 6, not spin on 0"
  end

  test "does NOT advance when the broker returned no batches (caught up)" do
    # No batches at all -> next_offset nil. The consumer must keep polling the
    # same offset, never skip ahead (which would lose messages).
    fetch =
      {:ok,
       %Fetch{
         topic: "t",
         partition: 0,
         records: [],
         last_offset: nil,
         next_offset: nil,
         high_watermark: 0
       }}

    offsets = fetch_offsets(run(fetch))

    assert offsets != []
    assert Enum.all?(offsets, &(&1 == 0)), "caught-up consumer must not advance off offset 0, got #{inspect(offsets)}"
  end
end
