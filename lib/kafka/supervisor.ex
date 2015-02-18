defmodule Kafka.Supervisor do
  use Supervisor

  def start_link do
    Supervisor.start_link(__MODULE__, [])
  end

  def init([]) do
    children = [
      worker(Kafka.Server, [Application.get_env(Kafka, :brokers)])
    ]

  supervise(children, strategy: :one_for_one)
  end
end
