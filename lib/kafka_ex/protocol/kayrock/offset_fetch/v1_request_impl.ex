defimpl KafkaEx.Protocol.Kayrock.OffsetFetch.Request, for: Kayrock.OffsetFetch.V1.Request do
  @moduledoc """
  Implementation for OffsetFetch v1 Request.

  This version is used for coordinator-based offset fetching from Kafka storage.
  """

  alias KafkaEx.Protocol.Kayrock.OffsetFetch.RequestHelpers

  def build_request(request_template, opts) do
    %{group_id: group_id} = RequestHelpers.extract_common_fields(opts)
    topics = RequestHelpers.build_topics(opts)

    request_template
    |> Map.put(:group_id, group_id)
    |> Map.put(:topics, topics)
  end
end
