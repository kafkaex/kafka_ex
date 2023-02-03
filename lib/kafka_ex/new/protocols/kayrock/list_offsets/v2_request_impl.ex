defimpl KafkaEx.New.Protocols.ListOffsets.Request,
  for: Kayrock.ListOffsets.V2.Request do
  def build_request(request_template, topic_partitions, opts \\ []) do
    isolation_level = Keyword.get(opts, :isolation_level, 1)

    %{
      request_template
      | replica_id: -1,
        isolation_level: isolation_level,
        topics: Enum.map(topic_partitions, &build_topic/1)
    }
  end

  defp build_topic({topic_name, partition_ids}) do
    %{
      topic: topic_name,
      partitions:
        Enum.map(partition_ids, fn partition_id ->
          %{partition: partition_id, timestamp: -1}
        end)
    }
  end
end
