defimpl KafkaEx.New.Protocols.Kayrock.OffsetCommit.Request, for: Kayrock.OffsetCommit.V2.Request do
  @moduledoc """
  Implementation for OffsetCommit v2 Request.

  This version stores offsets in Kafka (not Zookeeper) and:
  - Removes the timestamp field (Kafka manages timestamps)
  - Adds retention_time field for offset expiration
  This is the recommended version for modern Kafka deployments.
  """

  def build_request(request_template, opts) do
    group_id = Keyword.fetch!(opts, :group_id)
    generation_id = Keyword.get(opts, :generation_id, -1)
    member_id = Keyword.get(opts, :member_id, "")
    retention_time = Keyword.get(opts, :retention_time, -1)

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
                offset: partition_data.offset,
                metadata: partition_data[:metadata] || ""
              }
            end)
        }
      end)

    request_template
    |> Map.put(:group_id, group_id)
    |> Map.put(:generation_id, generation_id)
    |> Map.put(:member_id, member_id)
    |> Map.put(:retention_time, retention_time)
    |> Map.put(:topics, topics)
  end
end
