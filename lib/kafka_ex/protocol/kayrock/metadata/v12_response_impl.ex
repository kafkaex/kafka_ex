defimpl KafkaEx.Protocol.Kayrock.Metadata.Response, for: Kayrock.Metadata.V12.Response do
  @moduledoc """
  Implementation of Metadata Response protocol for Kafka V12 API.

  V12 makes the response topic `name` nullable: a topic requested by an
  unknown id comes back with a null name and an error code
  (`UNKNOWN_TOPIC_ID`). That is returned as `{:error, :unknown_topic_id}`
  with the unknown ids, rather than dropped (see
  `ResponseHelpers.to_cluster_metadata_by_id/1`). A topic without an error
  always carries both its name and id.
  """

  alias KafkaEx.Protocol.Kayrock.Metadata.ResponseHelpers

  def parse_response(response) do
    ResponseHelpers.to_cluster_metadata_by_id(response)
  end
end
