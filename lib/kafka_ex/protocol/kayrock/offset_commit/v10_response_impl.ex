defimpl KafkaEx.Protocol.Kayrock.OffsetCommit.Response, for: Kayrock.OffsetCommit.V10.Response do
  @moduledoc """
  Implementation for OffsetCommit V10 Response.

  Topics carry `topic_id` instead of `name`. `KafkaEx.Protocol.KayrockProtocol.parse_response/3`
  attaches the names from cluster metadata before dispatching here, so the
  result is the same `[KafkaEx.Messages.Offset.t()]` (keyed by topic name) as
  for earlier versions. A topic whose id could not be resolved returns
  `{:error, %KafkaEx.Client.Error{error: :unknown_topic_id}}`.
  """

  alias KafkaEx.Protocol.Kayrock.OffsetCommit.ResponseHelpers

  def parse_response(response) do
    ResponseHelpers.parse_topic_id_response(response)
  end
end
