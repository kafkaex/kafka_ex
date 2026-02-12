defimpl KafkaEx.Protocol.Kayrock.Metadata.Response, for: Kayrock.Metadata.V6.Response do
  @moduledoc """
  Implementation of Metadata Response protocol for Kafka V6 API.

  V6 response schema is identical to V5. No new fields added.
  """

  alias KafkaEx.Protocol.Kayrock.Metadata.ResponseHelpers

  @doc """
  Parses a V6 Metadata response into ClusterMetadata.
  """
  def parse_response(response) do
    ResponseHelpers.to_cluster_metadata(response)
  end
end
