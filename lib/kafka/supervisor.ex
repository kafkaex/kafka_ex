defmodule Kafka.Supervisor do
  use Supervisor

  def start_link do
    {:ok, pid} = Supervisor.start_link(__MODULE__, nil, [name: __MODULE__])
    uris = Application.get_env(Kafka, :brokers)
    Kafka.Supervisor.create_worker(uris, Kafka.Server)
    {:ok, pid}
  end

  def init(_) do
    children = [
      worker(Kafka.Server, [])
    ]
    supervise(children, strategy: :simple_one_for_one, max_restarts: 10, max_seconds: 60)
  end

  @doc """
  create_worker creates Kafka workers with broker list supplied in config
  """
  @spec create_worker(atom) :: pid
  def create_worker(name) do
    uris = Application.get_env(Kafka, :brokers)
    {:ok, pid} = Supervisor.start_child(__MODULE__, [uris, name])
    pid
  end

  @doc """
  create_worker creates Kafka workers
  """
  @spec create_worker(Kafka.Server.uri, atom) :: pid
  def create_worker(uris, name) do
    {:ok, pid} = Supervisor.start_child(__MODULE__, [uris, name])
    pid
  end

end
