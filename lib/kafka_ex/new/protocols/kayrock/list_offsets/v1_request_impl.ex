defimpl KafkaEx.New.Protocols.Kayrock.ListOffsets.Request, for: [Kayrock.ListOffsets.V1.Request] do
  def build_request(request_template, opts) do
    replica_id = Keyword.get(opts, :replica_id, -1)

    topics =
      opts
      |> Keyword.fetch!(:topics)
      |> Enum.map(fn {topic, partitions} ->
        %{
          topic: topic,
          partitions: Enum.map(partitions, &%{partition: &1.partition_num, timestamp: &1.timestamp})
        }
      end)

    request_template
    |> Map.put(:replica_id, replica_id)
    |> Map.put(:topics, topics)
  end
end
