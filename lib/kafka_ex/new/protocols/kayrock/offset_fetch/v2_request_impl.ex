defimpl KafkaEx.New.Protocols.Kayrock.OffsetFetch.Request, for: Kayrock.OffsetFetch.V2.Request do
  @moduledoc """
  Implementation of OffsetFetch Request protocol for API version 2.

  V2 adds a top-level error_code field to the response.
  Request structure is identical to V1.
  """

  alias KafkaEx.New.Protocols.Kayrock.OffsetFetch.RequestHelpers

  def build_request(request_template, opts) do
    %{group_id: group_id} = RequestHelpers.extract_common_fields(opts)
    topics = RequestHelpers.build_topics(opts)

    request_template
    |> Map.put(:group_id, group_id)
    |> Map.put(:topics, topics)
  end
end
