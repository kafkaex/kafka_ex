defmodule KafkaEx.Client.RequestBudgetTest do
  use ExUnit.Case, async: true

  alias KafkaEx.Client.RequestBudget

  @buffer 5_000

  describe "call_budget/2" do
    test "defaults to a single attempt (send-once): per_attempt + buffer" do
      assert RequestBudget.call_budget(95_000) == 95_000 + @buffer
      assert RequestBudget.call_budget(15_000) == 15_000 + @buffer
    end

    test "multiplies by attempts for a retried request" do
      assert RequestBudget.call_budget(15_000, 3) == 15_000 * 3 + @buffer
      assert RequestBudget.call_budget(10_000, 1) == 10_000 + @buffer
    end

    test "is always strictly larger than the per-attempt deadline" do
      for per_attempt <- [1, 1_000, 40_000, 95_000], attempts <- [1, 3, 6] do
        assert RequestBudget.call_budget(per_attempt, attempts) > per_attempt
      end
    end

    test "grows with both per_attempt and attempts" do
      assert RequestBudget.call_budget(100, 5) > RequestBudget.call_budget(100, 4)
      assert RequestBudget.call_budget(200, 3) > RequestBudget.call_budget(100, 3)
    end

    test "rejects non-positive inputs" do
      assert_raise FunctionClauseError, fn -> RequestBudget.call_budget(0) end
      assert_raise FunctionClauseError, fn -> RequestBudget.call_budget(-1) end
      assert_raise FunctionClauseError, fn -> RequestBudget.call_budget(100, 0) end
    end
  end
end
