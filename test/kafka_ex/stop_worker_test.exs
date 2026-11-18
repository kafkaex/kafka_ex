defmodule KafkaEx.StopWorkerTest do
  use ExUnit.Case, async: false

  defmodule StubWorker do
    use GenServer

    def start_link(_init, name), do: GenServer.start_link(__MODULE__, :ok, name: name)

    @impl true
    def init(:ok), do: {:ok, :ok}
  end

  setup do
    if Process.whereis(KafkaEx.Supervisor) == nil,
      do: start_supervised!(%{id: :sup, start: {KafkaEx.Supervisor, :start_link, [3, 5]}})

    name = :"stop_worker_#{System.unique_integer([:positive])}"
    {:ok, pid} = KafkaEx.Supervisor.start_child(StubWorker, [[], name])
    ref = Process.monitor(pid)

    %{name: name, pid: pid, ref: ref}
  end

  test "stops a worker by its registered name", %{name: name, pid: pid, ref: ref} do
    assert :ok = KafkaEx.stop_worker(name)

    assert_receive {:DOWN, ^ref, :process, ^pid, _reason}, 5_000
    assert Process.whereis(name) == nil
  end

  test "stops a worker by pid", %{pid: pid, ref: ref} do
    assert :ok = KafkaEx.stop_worker(pid)

    assert_receive {:DOWN, ^ref, :process, ^pid, _reason}, 5_000
  end

  test "returns :not_found for an unknown name" do
    assert {:error, :not_found} = KafkaEx.stop_worker(:no_such_worker)
  end

  test "returns :not_found for a pid that is not a child" do
    {:ok, stranger} = Agent.start_link(fn -> :ok end)

    assert {:error, :not_found} = KafkaEx.stop_worker(stranger)
  end
end
