defimpl KafkaEx.Protocol.Kayrock.Metadata.Response, for: Kayrock.Metadata.V12.Response do
  @moduledoc """
  Implementation of Metadata Response protocol for Kafka V12 API.

  V12 makes the response topic `name` nullable: a topic requested by an
  unknown id comes back with a null name and an error code
  (`UNKNOWN_TOPIC_ID`), and is dropped like any other errored topic. A topic
  without an error always carries both its name and id.
  """

  alias KafkaEx.Protocol.Kayrock.Metadata.ResponseHelpers

  def parse_response(response) do
    ResponseHelpers.to_cluster_metadata(response)
  end
end
