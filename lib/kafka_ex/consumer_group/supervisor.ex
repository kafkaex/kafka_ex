defmodule KafkaEx.ConsumerGroup.Supervisor do
  @moduledoc """
  A supervisor for managing a consumer group. A `KafkaEx.ConsumerGroup.Supervisor` process will
  manage an entire process tree for a single consumer group. Multiple supervisors can be used for
  multiple consumer groups within the same application.

  ## Example

  This supervisor can be addeded to an application's supervision tree with a custom `GenConsumer`
  implementation with the following child spec:

  ```
  supervisor(KafkaEx.ConsumerGroup.Supervisor, [MyApp.Consumer, "group_name", ["topic1", "topic2"]])
  ```
  """

  use Elixir.Supervisor

  @typedoc """
  Option values used when starting a `ConsumerGroup.Supervisor`.
  """
  @type option :: KafkaEx.GenConsumer.option
                | {:name, Elixir.Supervisor.name}
                | {:max_restarts, non_neg_integer}
                | {:max_seconds, non_neg_integer}

  @typedoc """
  Options used when starting a `ConsumerGroup.Supervisor`.
  """
  @type options :: [option]

  @doc """
  Starts a `ConsumerGroup.Supervisor` process linked to the current process.

  This can be used to start a `KafkaEx.ConsumerGroup` as part of a supervision tree.

  `module` is a module that implements the `KafkaEx.GenConsumer` behaviour. `group_name` is the
  name of the consumer group. `topics` is a list of topics that the consumer group should consume
  from. `opts` can be any options accepted by `KafkaEx.ConsumerGroup` or `Supervisor`.

  ### Return Values

  This function has the same return values as `Supervisor.start_link/3`.

  If the supervisor and consumer group are successfully created and initialized, this function
  returns `{:ok, pid}`, where `pid` is the PID of the consumer group supervisor process.
  """
  @spec start_link(module, binary, [binary], options) :: Elixir.Supervisor.on_start
  def start_link(consumer_module, group_name, topics, opts \\ []) do
    {supervisor_opts, module_opts} = Keyword.split(opts, [:name, :strategy, :max_restarts, :max_seconds])

    Elixir.Supervisor.start_link(__MODULE__, {consumer_module, group_name, topics, module_opts}, supervisor_opts)
  end

  @doc false # used by ConsumerGroup to set partition assignments
  def start_consumer(pid, consumer_module, group_name, assignments, opts) do
    child = supervisor(KafkaEx.GenConsumer.Supervisor, [consumer_module, group_name, assignments, opts], id: :consumer)

    case Elixir.Supervisor.start_child(pid, child) do
      {:ok, _child} -> :ok
      {:ok, _child, _info} -> :ok
    end
  end

  @doc false # used by ConsumerGroup to pause consumption during rebalance
  def stop_consumer(pid) do
    case Elixir.Supervisor.terminate_child(pid, :consumer) do
      :ok ->
        Elixir.Supervisor.delete_child(pid, :consumer)

      {:error, :not_found} ->
        :ok
    end
  end

  def init({consumer_module, group_name, topics, opts}) do
    opts = Keyword.put(opts, :supervisor_pid, self())

    children = [
      worker(KafkaEx.ConsumerGroup, [consumer_module, group_name, topics, opts]),
    ]

    supervise(children, strategy: :one_for_all)
  end
end
