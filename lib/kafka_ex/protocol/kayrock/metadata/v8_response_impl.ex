defimpl KafkaEx.Protocol.Kayrock.Metadata.Response, for: Kayrock.Metadata.V8.Response do
  @moduledoc """
  Implementation of Metadata Response protocol for Kafka V8 API.

  V8 adds `cluster_authorized_operations` (top-level) and per-topic
  `topic_authorized_operations`. These fields are parsed by Kayrock but
  not currently exposed in the ClusterMetadata or Topic domain structs.
  """

  alias KafkaEx.Protocol.Kayrock.Metadata.ResponseHelpers

  @doc """
  Parses a V8 Metadata response into ClusterMetadata.
  """
  def parse_response(response) do
    ResponseHelpers.to_cluster_metadata(response)
  end
end
