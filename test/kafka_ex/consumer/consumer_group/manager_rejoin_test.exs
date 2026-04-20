defmodule KafkaEx.Consumer.ConsumerGroup.ManagerRejoinTest do
  @moduledoc """
  Unit tests for Manager's rejoin_required handling.

  Specifically targets `drain_stale_rejoin_casts/2` — the mailbox coalescer
  that absorbs queued `:rejoin_required` casts tagged with the pre-rebalance
  generation. Casts tagged with the NEW generation (from fresh GenConsumers
  that hit a real fatal error post-rebalance) are preserved.

  The full-path cast handler is covered by integration tests.
  """
  use ExUnit.Case, async: true
  alias KafkaEx.Consumer.ConsumerGroup.Manager

  describe "drain_for_test/1 drops stale-generation casts" do
    test "drains all casts tagged with the stale generation" do
      Enum.each(1..7, fn _ ->
        send(self(), {:"$gen_cast", {:rejoin_required, :illegal_generation, 5}})
      end)

      assert Manager.drain_for_test(5) == 7
    end

    test "returns 0 when no matching casts are queued" do
      assert Manager.drain_for_test(5) == 0
    end

    test "preserves casts tagged with a DIFFERENT (fresh) generation" do
      # stale casts from old gen
      send(self(), {:"$gen_cast", {:rejoin_required, :illegal_generation, 5}})
      send(self(), {:"$gen_cast", {:rejoin_required, :illegal_generation, 5}})
      # fresh cast from new gen (should survive)
      send(self(), {:"$gen_cast", {:rejoin_required, :unknown_member_id, 6}})

      assert Manager.drain_for_test(5) == 2

      assert_received {:"$gen_cast", {:rejoin_required, :unknown_member_id, 6}}
    end

    test "drains legacy 2-tuple casts (no generation tag) as stale" do
      send(self(), {:"$gen_cast", {:rejoin_required, :illegal_generation}})
      send(self(), {:"$gen_cast", {:rejoin_required, :unknown_member_id}})

      assert Manager.drain_for_test(42) == 2
    end

    test "leaves non-matching messages in the mailbox" do
      send(self(), {:"$gen_cast", {:rejoin_required, :illegal_generation, 5}})
      send(self(), {:"$gen_cast", {:other_event, :foo}})
      send(self(), :random_message)
      send(self(), {:"$gen_cast", {:rejoin_required, :unknown_member_id, 5}})

      assert Manager.drain_for_test(5) == 2

      assert_received {:"$gen_cast", {:other_event, :foo}}
      assert_received :random_message
    end

    test "drains mixed fatal reasons at the same generation" do
      reasons = [:illegal_generation, :unknown_member_id]

      Enum.each(reasons, fn r ->
        send(self(), {:"$gen_cast", {:rejoin_required, r, 5}})
      end)

      assert Manager.drain_for_test(5) == 2
    end

    test "default arg (nil) still drains legacy 2-tuple form" do
      send(self(), {:"$gen_cast", {:rejoin_required, :illegal_generation}})
      assert Manager.drain_for_test() == 1
    end
  end
end
