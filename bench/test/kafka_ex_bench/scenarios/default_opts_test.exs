defmodule KafkaExBench.Scenarios.DefaultOptsTest do
  use ExUnit.Case, async: true
  alias KafkaExBench.Scenarios.{Basic, ColdStart, LargeFetch, Mixed, OneBroker, RestartRecovery}

  test "every scenario declares its default time and warmup" do
    assert Basic.default_opts()[:time] == 10
    assert Mixed.default_opts()[:time] == 30
    assert OneBroker.default_opts()[:time] == 10
    assert LargeFetch.default_opts()[:time] == 15
  end

  test "one-shot scenarios have no benchmark window" do
    assert ColdStart.default_opts()[:time] == 0
    assert RestartRecovery.default_opts()[:warmup] == 0
  end

  test "min_valid is 5 except restart recovery which needs 3" do
    for scenario <- [Basic, Mixed, OneBroker, LargeFetch, ColdStart],
        do: assert(scenario.default_opts()[:min_valid] == 5)

    assert RestartRecovery.default_opts()[:min_valid] == 3
  end

  test "mixed jobs need 30 samples and large fetch jobs 200" do
    mixed = Mixed.jobs(self(), %{idle: "i", produce: "p", payload: "x"})
    large = LargeFetch.jobs(self(), %{large: "l", small: "s", payload: "p"})

    assert Enum.all?(mixed, &(&1.min_samples == 30))
    assert Enum.all?(large, &(&1.min_samples == 200))
  end
end
