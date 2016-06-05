defmodule KafkaEx.Supervisor do
  use Supervisor

  def start_link(server, max_restarts, max_seconds) do
    {:ok, pid} = Supervisor.start_link(__MODULE__, [server, max_restarts, max_seconds], [name: __MODULE__])
    {:ok, pid}
  end

  def init([server, max_restarts, max_seconds]) do
    children = [
      worker(server, [])
    ]
    supervise(children, [strategy: :simple_one_for_one, max_restarts: max_restarts, max_seconds: max_seconds])
  end
end
