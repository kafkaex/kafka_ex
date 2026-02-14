defimpl KafkaEx.Protocol.Kayrock.OffsetFetch.Request, for: Kayrock.OffsetFetch.V6.Request do
  @moduledoc """
  Implementation of OffsetFetch Request protocol for API version 6.

  V6 is a flexible version (KIP-482) -- uses compact strings, compact arrays,
  and tagged fields. Kayrock handles the encoding transparently, so the
  request building logic is identical to V3-V5.
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
