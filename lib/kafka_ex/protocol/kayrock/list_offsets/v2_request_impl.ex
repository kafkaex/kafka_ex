defimpl KafkaEx.Protocol.Kayrock.ListOffsets.Request, for: [Kayrock.ListOffsets.V2.Request] do
  @isolation_level %{read_uncommited: 0, read_commited: 1}
  import KafkaEx.Protocol.Kayrock.ResponseHelpers, only: [parse_time: 1]

  def build_request(request_template, opts) do
    replica_id = Keyword.get(opts, :replica_id, -1)
    isolation_level = Keyword.get(opts, :isolation_level)
    kafka_isolation_level = Map.get(@isolation_level, isolation_level, 0)

    topics =
      opts
      |> Keyword.fetch!(:topics)
      |> Enum.map(fn {topic, partitions} ->
        %{
          topic: topic,
          partitions: Enum.map(partitions, &%{partition: &1.partition_num, timestamp: parse_time(&1.timestamp)})
        }
      end)

    request_template
    |> Map.put(:replica_id, replica_id)
    |> Map.put(:isolation_level, kafka_isolation_level)
    |> Map.put(:topics, topics)
  end
end
