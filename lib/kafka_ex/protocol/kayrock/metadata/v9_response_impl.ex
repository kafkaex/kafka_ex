defimpl KafkaEx.Protocol.Kayrock.Metadata.Response, for: Kayrock.Metadata.V9.Response do
  @moduledoc """
  Implementation of Metadata Response protocol for Kafka V9 API.

  V9 is a flexible version (KIP-482) that uses compact encodings:
  - Brokers use compact_array with compact_string/compact_nullable_string
  - Topics use compact_array with compact_string
  - Partitions use compact_array
  - Response includes top-level tagged_fields
  - Otherwise same logical fields as V8

  Tagged fields from the response are silently discarded since KafkaEx
  does not currently use any defined tagged field extensions.
  """

  alias KafkaEx.Protocol.Kayrock.Metadata.ResponseHelpers

  @doc """
  Parses a V9 Metadata response into ClusterMetadata.
  """
  def parse_response(response) do
    ResponseHelpers.to_cluster_metadata(response)
  end
end
