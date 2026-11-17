defmodule KafkaExBench.WorkersTest do
  use ExUnit.Case, async: true
  alias KafkaExBench.Workers

  defp alive?(pids), do: Enum.any?(pids, &Process.alive?/1)

  # A worker sends its error before it exits, so its DOWN arrives after the error message.
  defp await_exit(pid) do
    ref = Process.monitor(pid)
    assert_receive {:DOWN, ^ref, _, _, _}, 1000
  end

  test "stop returns the completed step count and terminates every worker when no step failed" do
    workers = Workers.start(3, & &1, fn state -> Process.sleep(5) && {:ok, state} end)
    Process.sleep(50)

    assert {:ok, ops} = Workers.stop(workers)
    assert ops >= 3
    Process.sleep(20)
    refute alive?(workers.pids)
  end

  test "stop reports the first error a worker saw with the steps completed so far" do
    workers =
      Workers.start(
        2,
        & &1,
        fn
          0 -> {:error, "first failure"}
          1 -> Process.sleep(200) && {:error, "later failure"}
        end
      )

    await_exit(hd(workers.pids))
    assert {:error, "first failure", 0} = Workers.stop(workers)
  end

  test "an exception in a step becomes an error" do
    workers = Workers.start(1, & &1, fn _ -> raise "boom" end)

    await_exit(hd(workers.pids))
    assert {:error, message, 0} = Workers.stop(workers)
    assert message =~ "boom"
  end

  test "a step returning something unexpected becomes an error" do
    workers = Workers.start(1, & &1, fn _ -> :weird end)

    await_exit(hd(workers.pids))
    assert {:error, message, 0} = Workers.stop(workers)
    assert message =~ "unexpected worker result"
  end

  test "workers carry their state from step to step and only successful steps are counted" do
    parent = self()

    workers =
      Workers.start(
        1,
        fn _ -> 0 end,
        fn
          3 -> send(parent, :reached_three) && {:error, "done"}
          n -> {:ok, n + 1}
        end
      )

    assert_receive :reached_three, 500
    assert {:error, "done", 3} = Workers.stop(workers)
  end

  test "stop does not consume messages of another worker group" do
    other = Workers.start(1, & &1, fn _ -> {:error, "other group"} end)
    await_exit(hd(other.pids))
    mine = Workers.start(1, & &1, fn s -> Process.sleep(5) && {:ok, s} end)

    assert {:ok, _} = Workers.stop(mine)
    assert {:error, "other group", 0} = Workers.stop(other)
  end

  test "each group counts its own steps" do
    busy = Workers.start(1, & &1, fn s -> Process.sleep(1) && {:ok, s} end)
    idle = Workers.start(1, & &1, fn s -> Process.sleep(:infinity) && {:ok, s} end)
    Process.sleep(50)

    assert {:ok, 0} = Workers.stop(idle)
    assert {:ok, ops} = Workers.stop(busy)
    assert ops > 0
  end
end
