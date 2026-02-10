defimpl KafkaEx.Protocol.Kayrock.OffsetCommit.Request, for: Kayrock.OffsetCommit.V0.Request do
  @moduledoc """
  Implementation for OffsetCommit v0 Request.

  This version is used for committing offsets to Zookeeper (legacy).
  Request includes: group_id, topics with partitions (offset + metadata).
  """

  alias KafkaEx.Protocol.Kayrock.OffsetCommit.RequestHelpers

  def build_request(request_template, opts) do
    %{group_id: group_id} = RequestHelpers.extract_common_fields(opts)
    topics = RequestHelpers.build_topics(opts, false)

    request_template
    |> Map.put(:group_id, group_id)
    |> Map.put(:topics, topics)
  end
end
