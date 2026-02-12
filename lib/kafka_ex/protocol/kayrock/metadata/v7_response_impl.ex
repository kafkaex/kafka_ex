defimpl KafkaEx.Protocol.Kayrock.Metadata.Response, for: Kayrock.Metadata.V7.Response do
  @moduledoc """
  Implementation of Metadata Response protocol for Kafka V7 API.

  V7 adds `leader_epoch` in partition metadata. This field is parsed by
  Kayrock but not currently exposed in the PartitionInfo domain struct.
  """

  alias KafkaEx.Protocol.Kayrock.Metadata.ResponseHelpers

  @doc """
  Parses a V7 Metadata response into ClusterMetadata.
  """
  def parse_response(response) do
    ResponseHelpers.to_cluster_metadata(response)
  end
end
