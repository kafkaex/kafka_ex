defmodule KafkaEx.Supervisor do
  use Supervisor

  def start_link(supervision_strategy) do
    {:ok, pid} = Supervisor.start_link(__MODULE__, supervision_strategy, [name: __MODULE__])
    {:ok, pid}
  end

  def init(supervision_strategy) do
    children = [
      worker(KafkaEx.Server, [])
    ]
    supervise(children, supervision_strategy)
  end
end
