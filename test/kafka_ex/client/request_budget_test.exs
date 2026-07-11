defmodule KafkaEx.Client.RequestBudgetTest do
  @moduledoc """
  Pure unit coverage for the outer GenServer.call budget arithmetic. This is the
  arithmetic the capturing-mock API tests cannot see (the third arg to
  GenServer.call is enforced client-side), so it is pinned directly here.
  """
  use ExUnit.Case, async: true

  alias KafkaEx.Client.RequestBudget

  @buffer 5_000

  describe "retrying/2" do
    test "is per_attempt * retries + buffer" do
      assert RequestBudget.retrying(15_000, 3) == 15_000 * 3 + @buffer
      assert RequestBudget.retrying(10_000, 1) == 10_000 + @buffer
      assert RequestBudget.retrying(1, 1) == 1 + @buffer
    end

    test "grows with both per_attempt and retries" do
      assert RequestBudget.retrying(100, 5) > RequestBudget.retrying(100, 4)
      assert RequestBudget.retrying(200, 3) > RequestBudget.retrying(100, 3)
    end

    test "rejects non-positive inputs" do
      assert_raise FunctionClauseError, fn -> RequestBudget.retrying(0, 3) end
      assert_raise FunctionClauseError, fn -> RequestBudget.retrying(100, 0) end
      assert_raise FunctionClauseError, fn -> RequestBudget.retrying(-1, 3) end
    end
  end

  describe "send_once/1" do
    test "is per_attempt + buffer" do
      assert RequestBudget.send_once(95_000) == 95_000 + @buffer
      assert RequestBudget.send_once(15_000) == 15_000 + @buffer
    end

    test "is strictly larger than the per-attempt deadline (caller can't exit first)" do
      for per_attempt <- [1, 1_000, 40_000, 95_000] do
        assert RequestBudget.send_once(per_attempt) > per_attempt
      end
    end

    test "rejects non-positive inputs" do
      assert_raise FunctionClauseError, fn -> RequestBudget.send_once(0) end
      assert_raise FunctionClauseError, fn -> RequestBudget.send_once(-1) end
    end
  end

  test "a send-once budget never exceeds the retrying budget for the same per-attempt (retries > 1)" do
    per_attempt = 15_000
    assert RequestBudget.send_once(per_attempt) < RequestBudget.retrying(per_attempt, 3)
  end
end
