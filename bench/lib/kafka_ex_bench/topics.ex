defmodule KafkaExBench.Topics do
  alias KafkaEx.API
  alias KafkaEx.Messages.CreateTopics

  def create!(client, prefix, opts) do
    {name, _layout} = create_with_layout!(client, prefix, opts)
    name
  end

  def create_with_layout!(client, prefix, opts) do
    name = "bench-#{prefix}-#{System.system_time(:millisecond)}-#{System.unique_integer([:positive])}"
    partitions = Keyword.fetch!(opts, :partitions)

    {:ok, result} =
      API.create_topic(client, name, create_options(opts))

    ensure_created!(result, name)

    layout = cleanup_on_failure(client, name, fn -> await_leaders!(client, name, partitions, 100) end)

    {name, layout}
  end

  def create_options(opts) do
    case Keyword.get(opts, :replica_assignment) do
      nil ->
        [
          num_partitions: Keyword.fetch!(opts, :partitions),
          replication_factor: Keyword.get(opts, :replication_factor, 3),
          timeout: 30_000
        ]

      assignment ->
        [num_partitions: -1, replication_factor: -1, replica_assignment: assignment, timeout: 30_000]
    end
  end

  def with_topic(client, prefix, opts, fun) do
    topic = create!(client, prefix, opts)

    result = cleanup_on_failure(client, topic, fn -> fun.(topic) end)
    delete!(client, topic)
    result
  end

  def cleanup_on_failure(client, topic, fun) do
    fun.()
  rescue
    original ->
      stacktrace = __STACKTRACE__
      delete_after_failure(client, topic)
      reraise original, stacktrace
  catch
    kind, reason ->
      stacktrace = __STACKTRACE__
      delete_after_failure(client, topic)
      :erlang.raise(kind, reason, stacktrace)
  end

  def ensure_created!(result, name) do
    if CreateTopics.success?(result) do
      :ok
    else
      raise "creating topic #{name} failed: #{inspect(CreateTopics.failed_topics(result))}"
    end
  end

  def delete!(client, topic) do
    {:ok, result} = API.delete_topic(client, topic, timeout: 30_000)
    ensure_deleted!(result, topic)
  end

  def ensure_deleted!(result, name) do
    case Enum.reject(result.topic_results, &(&1.error == :no_error)) do
      [] -> :ok
      failed -> raise "deleting topic #{name} failed: #{inspect(failed)}"
    end
  end

  def ensure_distinct_leaders!(leaders) do
    if leaders |> Map.values() |> Enum.uniq() |> length() == map_size(leaders) do
      :ok
    else
      raise "partition leaders are not on distinct brokers: #{inspect(leaders)}"
    end
  end

  def leaders!(client, topic) do
    {layout, _brokers} = fetch_layout!(client, topic)
    layout
  end

  def fetch_layout!(client, topic, attempts \\ 20)

  def fetch_layout!(_client, topic, 0), do: raise("topic #{topic} did not appear in broker metadata")

  def fetch_layout!(client, topic, attempts) do
    {:ok, metadata} = API.metadata(client, [topic], [])

    case classify(metadata, topic) do
      {:layout, layout} ->
        {layout, map_size(metadata.brokers)}

      :absent ->
        Process.sleep(100)
        fetch_layout!(client, topic, attempts - 1)
    end
  end

  def classify(%{topics: topics}, topic) do
    case topics do
      %{^topic => %{partition_leaders: leaders}} -> {:layout, leaders}
      _ -> :absent
    end
  end

  def delete_after_failure(client, topic) do
    delete!(client, topic)
  rescue
    error -> warn_cleanup(topic, Exception.message(error))
  catch
    kind, reason -> warn_cleanup(topic, "#{kind} #{inspect(reason)}")
  end

  defp warn_cleanup(topic, detail), do: IO.warn("could not delete topic #{topic} after a failure: #{detail}")

  defp await_leaders!(_client, topic, _partitions, 0), do: raise("partitions of #{topic} have no leader")

  defp await_leaders!(client, topic, partitions, attempts) do
    with {:ok, metadata} <- API.metadata(client, [topic], []),
         {:layout, leaders} <- classify(metadata, topic),
         true <- map_size(leaders) == partitions and Enum.all?(leaders, fn {_, node} -> node >= 0 end) do
      leaders
    else
      _ ->
        Process.sleep(100)
        await_leaders!(client, topic, partitions, attempts - 1)
    end
  end
end
