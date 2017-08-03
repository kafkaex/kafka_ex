defmodule KafkaEx.GenConsumer.Supervisor do
  @moduledoc """
  A supervisor for managing `GenConsumer` processes that are part of a consumer
  group.

  The supervisor will launch individual `GenConsumer` processes for each
  partition given by the `partitions` argument to `start_link/4`. When
  terminated, each of the supervisor's child processes will commit its latest
  offset before terminating.

  This module manages a static list of consumer processes. For dynamically
  distributing consumers in a consumer group across a cluster of nodes, see
  `KafkaEx.ConsumerGroup`.
  """

  use Elixir.Supervisor

  @doc """
  Starts a `GenConsumer.Supervisor` process linked to the current process.

  `module` is a module that implements the `GenConsumer` behaviour.
  `group_name` is the name of a consumer group, and `assignments` is a list of
  partitions for the `GenConsumer`s to consume.  `opts` accepts the same
  options as `KafkaEx.GenConsumer.start_link/5`.

  ### Return Values

  This function has the same return values as `Supervisor.start_link/3`.

  If the supervisor and its consumers are successfully created, this function
  returns `{:ok, pid}`, where `pid` is the PID of the supervisor.
  """
  @spec start_link(
    callback_module :: module,
    consumer_group_name :: binary,
    assigned_partitions :: [
      {topic_name :: binary, partition_id :: non_neg_integer}
    ],
    KafkaEx.GenConsumer.options
  ) :: Elixir.Supervisor.on_start
  def start_link(consumer_module, group_name, assignments, opts \\ []) do
    start_link_result = Elixir.Supervisor.start_link(
      __MODULE__,
      {consumer_module, group_name, assignments, opts}
    )
    case start_link_result do
      {:ok, pid} ->
        :ok = start_workers(pid, assignments, opts)
        {:ok, pid}

      error ->
        error
    end
  end

  def init({consumer_module, group_name, _assignments, _opts}) do
    children = [
      worker(KafkaEx.GenConsumer, [consumer_module, group_name])
    ]

    supervise(children, strategy: :simple_one_for_one)
  end

  defp start_workers(pid, assignments, opts) do
    Enum.each(assignments, fn ({topic, partition}) ->
      case Elixir.Supervisor.start_child(pid, [topic, partition, opts]) do
        {:ok, _child} -> nil
        {:ok, _child, _info} -> nil
      end
    end)

    :ok
  end
end
