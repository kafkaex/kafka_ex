defimpl KafkaEx.New.Protocols.Kayrock.Metadata.Response, for: Kayrock.Metadata.V2.Response do
  @moduledoc """
  Implementation of Metadata Response protocol for Kafka V2 API.
  """

  alias KafkaEx.New.Protocols.Kayrock.Metadata.ResponseHelpers

  @doc """
  Parses a V2 Metadata response into ClusterMetadata.
  """
  def parse_response(response) do
    ResponseHelpers.to_cluster_metadata(response)
  end
end
