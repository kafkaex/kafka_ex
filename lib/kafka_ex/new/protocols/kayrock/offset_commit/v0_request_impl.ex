defimpl KafkaEx.New.Protocols.Kayrock.OffsetCommit.Request, for: Kayrock.OffsetCommit.V0.Request do
  @moduledoc """
  Implementation for OffsetCommit v0 Request.

  This version is used for committing offsets to Zookeeper (legacy).
  Request includes: group_id, topics with partitions (offset + metadata).
  """

  def build_request(request_template, opts) do
    group_id = Keyword.fetch!(opts, :group_id)

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
    |> Map.put(:topics, topics)
  end
end
