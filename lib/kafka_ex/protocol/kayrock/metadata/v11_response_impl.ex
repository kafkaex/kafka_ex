defimpl KafkaEx.Protocol.Kayrock.Metadata.Response, for: Kayrock.Metadata.V11.Response do
  @moduledoc """
  Implementation of Metadata Response protocol for Kafka V11 API.

  V11 removes `cluster_authorized_operations` from the response. KafkaEx never
  surfaced it, so parsing is the same as V10 (topic ids included).
  """

  alias KafkaEx.Protocol.Kayrock.Metadata.ResponseHelpers

  def parse_response(response) do
    ResponseHelpers.to_cluster_metadata(response)
  end
end
