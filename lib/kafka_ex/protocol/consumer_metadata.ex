defmodule KafkaEx.Protocol.ConsumerMetadata do
  def create_request(correlation_id, client_id, consumer_group \\ "") do
    KafkaEx.Protocol.create_request(:consumer_metadata_request, correlation_id, client_id) <> << byte_size(consumer_group) :: 16 >> <> consumer_group
  end
end
