defimpl KafkaEx.Protocol.Kayrock.Metadata.Response, for: Kayrock.Metadata.V4.Response do
  @moduledoc """
  Implementation of Metadata Response protocol for Kafka V4 API.

  V4 response schema is identical to V3. The V4 change is on the request
  side (adding `allow_auto_topic_creation`).
  """

  alias KafkaEx.Protocol.Kayrock.Metadata.ResponseHelpers

  @doc """
  Parses a V4 Metadata response into ClusterMetadata.
  """
  def parse_response(response) do
    ResponseHelpers.to_cluster_metadata(response)
  end
end
