defimpl KafkaEx.Protocol.Kayrock.OffsetFetch.Request, for: Kayrock.OffsetFetch.V5.Request do
  @moduledoc """
  Implementation of OffsetFetch Request protocol for API version 5.

  V5 request structure is identical to V3/V4.
  The V5 response adds `committed_leader_epoch` per partition.
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
