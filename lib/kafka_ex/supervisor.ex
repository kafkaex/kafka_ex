alias KafkaEx.Config

defmodule KafkaEx.Supervisor do
  @moduledoc false

  use Elixir.DynamicSupervisor
  @supervisor __MODULE__

  def start_link(max_restarts, max_seconds) do
    {:ok, pid} =
      @supervisor
      |> Elixir.DynamicSupervisor.start_link(
        [max_restarts, max_seconds],
        name: __MODULE__
      )

    {:ok, pid}
  end

  def start_child(args) do
    module = Config.server_impl()

    @supervisor
    |> DynamicSupervisor.start_child({module, args})
  end

  def stop_child(child) do
    @supervisor |> Supervisor.terminate_child(child)
  end

  def init([max_restarts, max_seconds]) do
    DynamicSupervisor.init(
      strategy: :one_for_one,
      max_restarts: max_restarts,
      max_seconds: max_seconds
    )
  end
end
