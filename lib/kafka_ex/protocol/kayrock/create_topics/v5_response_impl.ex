defimpl KafkaEx.Protocol.Kayrock.CreateTopics.Response, for: Kayrock.CreateTopics.V5.Response do
  @moduledoc """
  Implementation for CreateTopics V5 Response.

  V5 is the flexible version (KIP-482) with compact types and tagged_fields.
  It adds new per-topic fields beyond V2-V4:
  - num_partitions: int32
  - replication_factor: int16
  - configs: compact_array of {name, value, read_only, config_source, is_sensitive, tagged_fields}
  - tagged_fields: tagged_fields per topic and at top level

  Uses `parse_v5_topic_results/1` which extracts these extra fields.
  """

  alias KafkaEx.Protocol.Kayrock.CreateTopics.ResponseHelpers

  def parse_response(%{throttle_time_ms: throttle_time_ms, topics: topics}) do
    topic_results = ResponseHelpers.parse_v5_topic_results(topics)
    {:ok, ResponseHelpers.build_response(topic_results, throttle_time_ms)}
  end
end
