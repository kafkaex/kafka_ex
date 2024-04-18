defimpl KafkaEx.New.Protocols.Kayrock.ListOffsets.Request, for: Kayrock.ListOffsets.V0.Request do
  import KafkaEx.New.Protocols.ListOffsets.Shared, only: [parse_time: 1]

  def build_request(request_template, opts) do
    replica_id = Keyword.get(opts, :replica_id, -1)
    offset_num = Keyword.get(opts, :offset_num, 1)

    topics =
      opts
      |> Keyword.fetch!(:topics)
      |> Enum.map(fn {topic, partitions} ->
        %{
          topic: topic,
          partitions:
            Enum.map(partitions, fn partition_data ->
              %{
                partition: partition_data.partition_num,
                timestamp: parse_time(partition_data.timestamp),
                max_num_offsets: offset_num
              }
            end)
        }
      end)

    request_template
    |> Map.put(:replica_id, replica_id)
    |> Map.put(:topics, topics)
  end
end
