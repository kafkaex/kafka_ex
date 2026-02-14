defimpl KafkaEx.Protocol.Kayrock.Metadata.Response, for: Kayrock.Metadata.V5.Response do
  @moduledoc """
  Implementation of Metadata Response protocol for Kafka V5 API.

  V5 adds `offline_replicas` in partition metadata. This field is parsed by
  Kayrock but not currently exposed in the PartitionInfo domain struct.
  """

  alias KafkaEx.Protocol.Kayrock.Metadata.ResponseHelpers

  @doc """
  Parses a V5 Metadata response into ClusterMetadata.
  """
  def parse_response(response) do
    ResponseHelpers.to_cluster_metadata(response)
  end
end
