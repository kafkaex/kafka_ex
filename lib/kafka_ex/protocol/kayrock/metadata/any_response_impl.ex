defimpl KafkaEx.Protocol.Kayrock.Metadata.Response, for: Any do
  @moduledoc """
  Fallback implementation of Metadata Response protocol for unknown future versions.

  All known Kayrock-supported versions (V0-V9) have explicit protocol
  implementations. This `Any` fallback exists solely for forward compatibility
  with potential future Kayrock versions that don't yet have an explicit impl.

  Delegates to `ResponseHelpers.to_cluster_metadata/1` which handles
  all standard response fields (brokers, topics, controller_id).
  """

  alias KafkaEx.Protocol.Kayrock.Metadata.ResponseHelpers

  @doc """
  Parses a Metadata response into ClusterMetadata.
  """
  def parse_response(response) do
    ResponseHelpers.to_cluster_metadata(response)
  end
end
