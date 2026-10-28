defimpl KafkaEx.Protocol.Kayrock.Metadata.Request, for: Kayrock.Metadata.V10.Request do
  @moduledoc """
  Implementation of Metadata Request protocol for Kafka V10 API.

  V10 adds a `topic_id` (uuid) to every request topic and makes the topic
  `name` nullable. The Kafka spec says V10 must not use topic-id lookups
  (brokers never implemented them for V10/V11), so every topic is sent by name
  with the null (all-zero) topic id. Other fields are as in V9.
  """

  alias KafkaEx.Protocol.Kayrock.Metadata.RequestHelpers

  def build_request(request_template, opts) do
    RequestHelpers.build_v10_request(request_template, opts)
  end
end
