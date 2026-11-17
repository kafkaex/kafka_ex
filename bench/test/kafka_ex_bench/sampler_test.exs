defmodule KafkaExBench.SamplerTest do
  use ExUnit.Case, async: true
  alias KafkaExBench.Sampler

  defp blocked_target(message_count) do
    ref = make_ref()
    pid = spawn(fn -> receive do: (^ref -> :ok) end)
    for i <- 1..message_count, do: send(pid, {:noise, i})
    pid
  end

  defp busy_target, do: spawn(&busy_loop/0)

  defp busy_loop, do: busy_loop()

  test "records the peak mailbox length of an unreceived backlog" do
    target = blocked_target(50)
    sampler = Sampler.start(target, 5)
    Process.sleep(20)
    summary = Sampler.stop(sampler)

    assert summary.max_mailbox >= 50
    assert summary.samples >= 1
  end

  test "records reductions spent by a busy target" do
    target = busy_target()
    on_exit(fn -> Process.exit(target, :kill) end)
    sampler = Sampler.start(target, 5)
    Process.sleep(80)
    summary = Sampler.stop(sampler)

    assert summary.reductions > 0
    assert summary.samples > 1
  end

  test "a target that dies keeps its last observed values" do
    target = blocked_target(10)
    sampler = Sampler.start(target, 5)
    Process.sleep(20)
    Process.exit(target, :kill)
    Process.sleep(20)
    summary = Sampler.stop(sampler)

    assert summary.max_mailbox >= 10
    assert summary.samples >= 1
  end

  test "a target already dead yields an empty summary" do
    target = spawn(fn -> :ok end)
    ref = Process.monitor(target)
    assert_receive {:DOWN, ^ref, _, _, _}

    assert Sampler.stop(Sampler.start(target, 5)) == %{max_mailbox: 0, reductions: 0, samples: 0}
  end

  test "stop leaves no sampler process alive" do
    sampler = Sampler.start(blocked_target(1), 5)
    ref = Process.monitor(sampler)
    Sampler.stop(sampler)

    assert_receive {:DOWN, ^ref, _, _, _}, 1000
    refute Process.alive?(sampler)
  end

  test "stop raises when the sampler is gone" do
    sampler = spawn(fn -> :ok end)
    ref = Process.monitor(sampler)
    assert_receive {:DOWN, ^ref, _, _, _}

    assert_raise RuntimeError, ~r/sampler/, fn -> Sampler.stop(sampler) end
  end
end
