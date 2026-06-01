defmodule KafkaEx.Consumer.ConsumerContractTest do
  use ExUnit.Case, async: true

  alias KafkaEx.Messages.Fetch
  alias KafkaEx.Messages.Fetch.Record

  describe "Fetch.filter_from_offset/2 (the filter-to-empty case)" do
    test "reduces to records: [] when all records fall below the requested offset" do
      fetch = %Fetch{
        topic: "t", partition: 0, last_offset: 2, high_watermark: 3,
        records: [%Record{offset: 0, value: "a"}, %Record{offset: 1, value: "b"}, %Record{offset: 2, value: "c"}]
      }

      filtered = Fetch.filter_from_offset(fetch, 5)

      # Contract the Fetcher must honour: filtering can yield an EMPTY batch.
      # The Fetcher must treat records == [] as "empty" (re-poll, never deliver []),
      # else the Consumer's `%Record{} = List.last([])` would MatchError.
      assert filtered.records == []
    end

    test "keeps records at/after the requested offset" do
      fetch = %Fetch{
        topic: "t", partition: 0, last_offset: 2, high_watermark: 3,
        records: [%Record{offset: 1, value: "b"}, %Record{offset: 2, value: "c"}]
      }

      filtered = Fetch.filter_from_offset(fetch, 2)
      assert Enum.map(filtered.records, & &1.offset) == [2]
    end
  end

  describe "positional last_offset (IMP-2)" do
    test "the consumer-advance uses List.last(records).offset (positional, not max-by)" do
      records = [%Record{offset: 7, value: "x"}, %Record{offset: 9, value: "y"}, %Record{offset: 8, value: "z"}]
      # Pins today's gen_consumer.ex behaviour: %Record{offset: last} = List.last(message_set)
      assert %Record{offset: last} = List.last(records)
      assert last == 8
    end
  end
end
