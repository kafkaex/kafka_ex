defmodule KafkaEx.SupervisorTest do
  use ExUnit.Case, async: false

  alias KafkaEx.Supervisor, as: KafkaExSupervisor

  # Mock module for testing
  defmodule MockWorker do
    use GenServer

    def start_link(arg) do
      GenServer.start_link(__MODULE__, arg)
    end

    def init(arg) do
      {:ok, arg}
    end
  end

  describe "start_link/2" do
    test "starts supervisor with max_restarts and max_seconds" do
      # Stop existing supervisor if running
      case Process.whereis(KafkaExSupervisor) do
        nil -> :ok
        pid -> GenServer.stop(pid)
      end

      assert {:ok, pid} = KafkaExSupervisor.start_link(3, 5)
      assert Process.alive?(pid)
      assert Process.whereis(KafkaExSupervisor) == pid

      # Cleanup
      GenServer.stop(pid)
    end
  end

  describe "start_child/2" do
    setup do
      # Stop existing supervisor if running
      case Process.whereis(KafkaExSupervisor) do
        nil ->
          :ok

        pid ->
          try do
            GenServer.stop(pid)
          catch
            :exit, _ -> :ok
          end
      end

      {:ok, sup_pid} = KafkaExSupervisor.start_link(3, 5)

      on_exit(fn ->
        try do
          if Process.alive?(sup_pid), do: GenServer.stop(sup_pid)
        catch
          :exit, _ -> :ok
        end
      end)

      {:ok, supervisor: sup_pid}
    end

    test "starts a child process" do
      assert {:ok, child_pid} = KafkaExSupervisor.start_child(MockWorker, [:test_arg])
      assert Process.alive?(child_pid)
    end

    test "can start multiple children" do
      {:ok, child1} = KafkaExSupervisor.start_child(MockWorker, [:arg1])
      {:ok, child2} = KafkaExSupervisor.start_child(MockWorker, [:arg2])

      assert Process.alive?(child1)
      assert Process.alive?(child2)
      assert child1 != child2
    end
  end

  describe "stop_child/1" do
    setup do
      # Stop existing supervisor if running
      case Process.whereis(KafkaExSupervisor) do
        nil ->
          :ok

        pid ->
          try do
            GenServer.stop(pid)
          catch
            :exit, _ -> :ok
          end
      end

      {:ok, sup_pid} = KafkaExSupervisor.start_link(3, 5)

      on_exit(fn ->
        try do
          if Process.alive?(sup_pid), do: GenServer.stop(sup_pid)
        catch
          :exit, _ -> :ok
        end
      end)

      {:ok, supervisor: sup_pid}
    end

    test "terminates a child process" do
      {:ok, child_pid} = KafkaExSupervisor.start_child(MockWorker, [:test])

      assert Process.alive?(child_pid)
      assert :ok = KafkaExSupervisor.stop_child(child_pid)

      # Give process time to terminate
      Process.sleep(10)
      refute Process.alive?(child_pid)
    end
  end

  describe "init/1" do
    test "initializes with one_for_one strategy" do
      assert {:ok, config} = KafkaExSupervisor.init([3, 5])

      assert config.strategy == :one_for_one
      # DynamicSupervisor uses intensity/period instead of max_restarts/max_seconds
      assert config.intensity == 3
      assert config.period == 5
    end
  end
end
