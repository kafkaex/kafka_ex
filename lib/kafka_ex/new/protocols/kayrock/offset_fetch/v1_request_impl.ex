defimpl KafkaEx.New.Protocols.Kayrock.OffsetFetch.Request, for: Kayrock.OffsetFetch.V1.Request do
  @moduledoc """
  Implementation for OffsetFetch v1 Request.

  This version is used for coordinator-based offset fetching from Kafka storage.
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
