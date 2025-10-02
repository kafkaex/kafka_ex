defimpl KafkaEx.New.Protocols.Kayrock.OffsetFetch.Request, for: Kayrock.OffsetFetch.V2.Request do
  @moduledoc """
  Implementation of OffsetFetch Request protocol for API version 2.

  V2 adds a top-level error_code field to the response.
  Request structure is identical to V1.
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
