defmodule KafkaEx.Supervisor do
  @moduledoc false

  use DynamicSupervisor

  def start_link(max_restarts, max_seconds) do
    {:ok, pid} =
      DynamicSupervisor.start_link(
        __MODULE__,
        [max_restarts, max_seconds],
        name: __MODULE__
      )

    {:ok, pid}
  end

  def start_child(impl, args) when is_atom(impl) and is_list(args) do
    spec = %{id: impl, start: {impl, :start_link, args}}
    DynamicSupervisor.start_child(__MODULE__, spec)
  end

  def stop_child(name) when is_atom(name) do
    case Process.whereis(name) do
      nil -> {:error, :not_found}
      pid -> stop_child(pid)
    end
  end

  def stop_child(pid) when is_pid(pid) do
    DynamicSupervisor.terminate_child(__MODULE__, pid)
  end

  def init([max_restarts, max_seconds]) do
    DynamicSupervisor.init(
      strategy: :one_for_one,
      max_restarts: max_restarts,
      max_seconds: max_seconds
    )
  end
end
