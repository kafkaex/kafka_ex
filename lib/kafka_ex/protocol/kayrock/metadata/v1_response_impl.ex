defimpl KafkaEx.Protocol.Kayrock.Metadata.Response, for: Kayrock.Metadata.V1.Response do
  @moduledoc """
  Implementation of Metadata Response protocol for Kafka V1 API.
  """

  alias KafkaEx.Protocol.Kayrock.Metadata.ResponseHelpers

  @doc """
  Parses a V1 Metadata response into ClusterMetadata.
  """
  def parse_response(response) do
    ResponseHelpers.to_cluster_metadata(response)
  end
end
