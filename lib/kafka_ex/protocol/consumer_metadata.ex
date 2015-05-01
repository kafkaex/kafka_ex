defmodule KafkaEx.Protocol.ConsumerMetadata do
  def create_request(correlation_id, client_id, consumer_group \\ "") do
    KafkaEx.Protocol.create_request(:consumer_metadata, correlation_id, client_id) <> << byte_size(consumer_group) :: 16, consumer_group :: binary >>
  end

  def parse_response(<< _corr_id :: 32, error_code :: 16, coord_id :: 32, coord_host_size :: 16, coord_host :: size(coord_host_size)-binary, coord_port :: 32, rest :: binary >>) do
    %{coordinator_id: coord_id, coordinator_host: coord_host, coordinator_port: coord_port}
  end
end
