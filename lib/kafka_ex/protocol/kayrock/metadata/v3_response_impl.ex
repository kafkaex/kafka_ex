defimpl KafkaEx.Protocol.Kayrock.Metadata.Response, for: Kayrock.Metadata.V3.Response do
  @moduledoc """
  Implementation of Metadata Response protocol for Kafka V3 API.

  V3 adds `throttle_time_ms` and `cluster_id` to the response.
  The `cluster_id` and `throttle_time_ms` are not currently exposed
  in the ClusterMetadata domain struct but are correctly parsed by Kayrock.
  """

  alias KafkaEx.Protocol.Kayrock.Metadata.ResponseHelpers

  @doc """
  Parses a V3 Metadata response into ClusterMetadata.
  """
  def parse_response(response) do
    ResponseHelpers.to_cluster_metadata(response)
  end
end
