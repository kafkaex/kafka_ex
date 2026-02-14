defimpl KafkaEx.Protocol.Kayrock.OffsetFetch.Request, for: Kayrock.OffsetFetch.V4.Request do
  @moduledoc """
  Implementation of OffsetFetch Request protocol for API version 4.

  V4 is a pure version bump -- request structure is identical to V3.
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
