defimpl KafkaEx.Protocol.Kayrock.Metadata.Response, for: Kayrock.Metadata.V10.Response do
  @moduledoc """
  Implementation of Metadata Response protocol for Kafka V10 API.

  V10 adds `topic_id` (16-byte uuid) to each topic. It is stored on
  `KafkaEx.Cluster.Topic.topic_id` (`nil` for the all-zero null id).
  Otherwise the same as V9.
  """

  alias KafkaEx.Protocol.Kayrock.Metadata.ResponseHelpers

  def parse_response(response) do
    ResponseHelpers.to_cluster_metadata(response)
  end
end
