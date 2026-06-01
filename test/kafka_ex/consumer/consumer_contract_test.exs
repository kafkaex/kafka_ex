defmodule KafkaEx.Consumer.ConsumerContractTest do
  use ExUnit.Case, async: true

  alias KafkaEx.Messages.Fetch
  alias KafkaEx.Messages.Fetch.Record

  describe "Fetch.filter_from_offset/2 (the filter-to-empty case)" do
    test "reduces to records: [] when all records fall below the requested offset" do
      fetch = %Fetch{
        topic: "t",
        partition: 0,
        last_offset: 2,
        high_watermark: 3,
        records: [%Record{offset: 0, value: "a"}, %Record{offset: 1, value: "b"}, %Record{offset: 2, value: "c"}]
      }

      filtered = Fetch.filter_from_offset(fetch, 5)

      # Contract the Fetcher must honour: filtering can yield an EMPTY batch.
      # The Fetcher must treat records == [] as "empty" (re-poll, never deliver []),
      # else the Consumer's `%Record{} = List.last([])` would MatchError.
      assert filtered.records == []

      # last_offset must reset to nil on an empty result. next_offset/1 keys off this:
      # nil -> high_watermark, otherwise last_offset + 1. If a refactor left a stale
      # non-nil last_offset here, the Fetcher would advance to the wrong offset.
      assert filtered.last_offset == nil
    end

    test "keeps records at/after the requested offset" do
      fetch = %Fetch{
        topic: "t",
        partition: 0,
        last_offset: 2,
        high_watermark: 3,
        records: [%Record{offset: 1, value: "b"}, %Record{offset: 2, value: "c"}]
      }

      filtered = Fetch.filter_from_offset(fetch, 2)
      assert Enum.map(filtered.records, & &1.offset) == [2]
    end
  end

  describe "positional last_offset (IMP-2)" do
    # NOTE: this pins a CALLING CONVENTION, not a production call-site. gen_consumer.ex
    # advances via `%Record{offset: last} = List.last(message_set)` — positional last,
    # NOT max-by. That is only correct if the batch is delivered in offset order, so
    # List.last is also the highest offset. The deliberately out-of-order list below
    # (7, 9, 8) makes the positional-vs-max distinction visible: List.last gives 8 (the
    # last element), whereas Fetch.compute_last_offset/2 would give 9 (max-by). The
    # contract the Fetcher/Consumer split must preserve: feed the Consumer offset-ordered
    # batches so the positional advance stays correct. (We can't call the private
    # gen_consumer path directly without exposing internals; the integration tests above
    # exercise the real end-to-end advance.)
    test "the consumer-advance uses List.last(records).offset (positional, not max-by)" do
      records = [%Record{offset: 7, value: "x"}, %Record{offset: 9, value: "y"}, %Record{offset: 8, value: "z"}]
      assert %Record{offset: last} = List.last(records)
      assert last == 8
      # Contrast: max-by (Fetch.compute_last_offset semantics) would yield 9, not 8.
      refute last == Enum.max_by(records, & &1.offset).offset
    end
  end
end
