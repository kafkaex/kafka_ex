defimpl KafkaEx.New.Protocols.Kayrock.OffsetCommit.Request, for: Kayrock.OffsetCommit.V1.Request do
  @moduledoc """
  Implementation for OffsetCommit v1 Request.

  This version adds timestamp, generation_id, and member_id for consumer group coordination.
  Can store offsets in either Kafka or Zookeeper depending on configuration.
  """

  def build_request(request_template, opts) do
    group_id = Keyword.fetch!(opts, :group_id)
    generation_id = Keyword.get(opts, :generation_id, -1)
    member_id = Keyword.get(opts, :member_id, "")

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
                timestamp: partition_data[:timestamp] || -1,
                metadata: partition_data[:metadata] || ""
              }
            end)
        }
      end)

    request_template
    |> Map.put(:group_id, group_id)
    |> Map.put(:generation_id, generation_id)
    |> Map.put(:member_id, member_id)
    |> Map.put(:topics, topics)
  end
end
