defmodule KafkaEx.Supervisor do
  use Supervisor

  def start_link do
    {:ok, pid} = Supervisor.start_link(__MODULE__, nil, [name: __MODULE__])
    {:ok, pid}
  end

  def init(_) do
    children = [
      worker(KafkaEx.Server, [])
    ]
    supervise(children, strategy: :simple_one_for_one, max_restarts: 10, max_seconds: 60)
  end
end
