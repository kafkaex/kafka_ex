defmodule KafkaEx.Client.Front.PropertiesTest do
  use ExUnit.Case, async: true
  use ExUnitProperties

  alias KafkaEx.TestSupport.FrontModel
  alias KafkaEx.TestSupport.FrontTrace

  @seed String.to_integer(System.get_env("KAFKA_EX_PROPERTY_SEED", "1494"))
  @runs String.to_integer(System.get_env("KAFKA_EX_PROPERTY_RUNS", "3000"))

  property "every entry ends exactly once, by its deadline, and nothing is dispatched at or after it" do
    check all(s <- FrontModel.scenario(), initial_seed: @seed, max_runs: @runs) do
      assert FrontTrace.termination(FrontModel.run(s)) == []
    end
  end

  property "only written failures cost an attempt, and no request exceeds its budget" do
    check all(s <- FrontModel.scenario(), initial_seed: @seed, max_runs: @runs) do
      assert FrontTrace.attempts(FrontModel.run(s)) == []
    end
  end

  property "a send-once request is never resent after a written failure in its set" do
    check all(s <- FrontModel.scenario(), initial_seed: @seed, max_runs: @runs) do
      assert FrontTrace.no_resend(FrontModel.run(s)) == []
    end
  end

  property "two dispatches of one entry are spaced by the backoff unless a wake allows otherwise" do
    check all(s <- FrontModel.scenario(), initial_seed: @seed, max_runs: @runs) do
      assert FrontTrace.backoff(FrontModel.run(s)) == []
    end
  end

  property "the produce gate is FIFO, holds at most N on the wire and leaks no slot" do
    check all(s <- FrontModel.scenario(), initial_seed: @seed, max_runs: @runs) do
      model = FrontModel.run(s, checks: [&FrontTrace.gate_check/1])
      assert FrontTrace.gate_order(model) == []
      assert KafkaEx.Client.Front.Core.idle?(model.core)
    end
  end

  property "nothing is dispatched for an entry whose caller is down" do
    check all(s <- FrontModel.scenario(), initial_seed: @seed, max_runs: @runs) do
      assert FrontTrace.dead_callers(FrontModel.run(s)) == []
    end
  end

  property "a first_available walk tries each candidate at most once" do
    check all(s <- FrontModel.scenario(), initial_seed: @seed, max_runs: @runs) do
      assert FrontTrace.first_available(FrontModel.run(s)) == []
    end
  end

  property "an exhausted first_available walk replies :broker_failed after a write, and otherwise waits" do
    check all(s <- FrontModel.scenario(), initial_seed: @seed, max_runs: @runs) do
      assert FrontTrace.walk_end(FrontModel.run(s)) == []
    end
  end

  property "an encode failure replies :timeout at once, with no retry and no actions" do
    check all(s <- FrontModel.scenario(), initial_seed: @seed, max_runs: @runs) do
      assert FrontTrace.encode_failure(FrontModel.run(s)) == []
    end
  end

  property "a deadline reply is :timeout in flight, else the last written failure, else :no_broker when parked" do
    check all(s <- FrontModel.scenario(), initial_seed: @seed, max_runs: @runs) do
      assert FrontTrace.deadline_reply(FrontModel.run(s)) == []
    end
  end

  property "no reply carries the error of a preferred replica" do
    check all(s <- FrontModel.scenario(), initial_seed: @seed, max_runs: @runs) do
      assert FrontTrace.replica_reply(FrontModel.run(s)) == []
    end
  end

  property "a failure is classified as the error its shape names, a connection failure as :transport_error" do
    check all(s <- FrontModel.scenario(), initial_seed: @seed, max_runs: @runs) do
      assert FrontTrace.classification(FrontModel.run(s)) == []
    end
  end

  property ":serial admits one entry at a time, in arrival order" do
    check all(s <- FrontModel.scenario(), initial_seed: @seed, max_runs: @runs) do
      model = FrontModel.run(s, concurrency: :serial)
      assert FrontTrace.serial_exclusive(model) == []
      assert FrontTrace.serial_order(model) == []
      assert FrontTrace.termination(model) == []
    end
  end
end
