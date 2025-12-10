defimpl KafkaEx.New.Protocols.Kayrock.Metadata.Response, for: Kayrock.Metadata.V0.Response do
  @moduledoc """
  Implementation of Metadata Response protocol for Kafka V0 API.
  """

  alias KafkaEx.New.Protocols.Kayrock.Metadata.ResponseHelpers

  @doc """
  Parses a V0 Metadata response into ClusterMetadata.
  """
  def parse_response(response) do
    ResponseHelpers.to_cluster_metadata(response)
  end
end
