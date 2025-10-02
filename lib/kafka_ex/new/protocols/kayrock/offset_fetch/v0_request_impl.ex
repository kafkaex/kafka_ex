defimpl KafkaEx.New.Protocols.Kayrock.OffsetFetch.Request, for: Kayrock.OffsetFetch.V0.Request do
  @moduledoc """
  Implementation for OffsetFetch v0 Request.

  This version is used for fetching offsets from Zookeeper (legacy).
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
              %{partition: partition_data.partition_num}
            end)
        }
      end)

    request_template
    |> Map.put(:group_id, group_id)
    |> Map.put(:topics, topics)
  end
end
