defmodule KafkaEx.Client.AdmissionTest do
  use ExUnit.Case, async: true

  alias KafkaEx.Client.Admission
  alias KafkaEx.Client.RequestBudget

  @now 10_000
  @default 7_000

  describe "admit/4" do
    test "a local caller's deadline counts from its send time, and the budget leaves the opts" do
      message = {:produce, "t", 0, [], [acks: 1, call_budget: {3_000, 9_000}]}

      assert Admission.admit(message, true, @now, @default) == {{:produce, "t", 0, [], [acks: 1]}, 12_000}
    end

    test "a remote caller's deadline counts from admission" do
      message = {:produce, "t", 0, [], [call_budget: {3_000, 9_000}]}

      assert Admission.admit(message, false, @now, @default) == {{:produce, "t", 0, [], []}, 13_000}
    end

    test "a message without a budget gets the default from admission" do
      assert Admission.admit({:produce, "t", 0, [], []}, true, @now, @default) ==
               {{:produce, "t", 0, [], []}, @now + @default}
    end

    test "the old topic_metadata shape becomes the keyword shape" do
      assert Admission.admit({:topic_metadata, ["t"], true}, true, @now, @default) ==
               {{:topic_metadata, ["t"], [allow_topic_creation: true]}, @now + @default}
    end

    test "the old topic_metadata shape passes any creation flag on as given" do
      for flag <- [nil, :yes, 1] do
        assert Admission.admit({:topic_metadata, ["t"], flag}, true, @now, @default) ==
                 {{:topic_metadata, ["t"], [allow_topic_creation: flag]}, @now + @default}
      end
    end

    test "opts that are not a keyword list still lose the budget" do
      assert Admission.admit({:produce, "t", 0, [], [:x, call_budget: {3_000, 9_000}]}, true, @now, @default) ==
               {{:produce, "t", 0, [], [:x]}, 12_000}

      assert Admission.admit({:metadata, nil, [:x, call_budget: {3_000, 9_000}], 2}, true, @now, @default) ==
               {{:metadata, nil, [:x], 2}, 12_000}
    end

    test "every call_budget entry leaves the opts and the first one counts" do
      message = {:produce, "t", 0, [], [call_budget: {3_000, 9_000}, acks: 1, call_budget: {1_000, 9_500}]}

      assert Admission.admit(message, true, @now, @default) == {{:produce, "t", 0, [], [acks: 1]}, 12_000}
    end

    test "elements that are not 2-tuples stay in the opts and do not raise" do
      opts = [:x, {:call_budget, 1, 2}, {:call_budget, {3_000, 9_000}}, {:a}, "s"]

      assert Admission.admit({:produce, "t", 0, [], opts}, true, @now, @default) ==
               {{:produce, "t", 0, [], [:x, {:call_budget, 1, 2}, {:a}, "s"]}, 12_000}
    end

    test "the old network_request shape gains empty opts" do
      assert Admission.admit({:network_request, :request, :selector}, true, @now, @default) ==
               {{:network_request, :request, :selector, []}, @now + @default}
    end

    test "metadata carries its opts third, before the api version" do
      message = {:metadata, nil, [call_budget: {3_000, 9_000}, api_version: 2], 2}

      assert Admission.admit(message, true, @now, @default) == {{:metadata, nil, [api_version: 2], 2}, 12_000}
    end

    test "messages without opts come back unchanged with the default" do
      for message <- [{:offset, "t", 0, -1}, :update_metadata, {:set_consumer_group_for_auto_commit, "g"}] do
        assert Admission.admit(message, true, @now, @default) == {message, @now + @default}
      end
    end
  end

  describe "deadline/4" do
    test "an unusable budget gets the default from admission" do
      for budget <- [5, {1, 2, 3}, {:a, :b}, "x"] do
        assert Admission.deadline(budget, true, @now, @default) == @now + @default
      end
    end

    test "a message with an unusable budget is admitted with the default" do
      assert Admission.admit({:produce, "t", 0, [], [call_budget: 5]}, true, @now, @default) ==
               {{:produce, "t", 0, [], []}, @now + @default}
    end

    test "is strictly earlier than a local caller's timeout, whatever the mailbox wait" do
      call_timeout = 6_000
      budget = {RequestBudget.deadline_budget(call_timeout), 0}

      assert Admission.deadline(budget, true, 5_500, @default) == 5_000
      assert Admission.deadline(budget, true, 5_500, @default) < 0 + call_timeout
    end
  end
end
