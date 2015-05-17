defmodule KafkaEx.Protocol.ConsumerMetadata do
  defmodule Response do
    defstruct coordinator_id: 0, coordinator_host: "", coordinator_port: 0, error_code: 0
    @type t :: %Response{coordinator_id: integer, coordinator_host: binary, coordinator_port: 0..65535, error_code: integer}
  end

  @spec create_request(integer, binary, binary) :: binary
  def create_request(correlation_id, client_id, consumer_group \\ "kafka_ex") do
    KafkaEx.Protocol.create_request(:consumer_metadata, correlation_id, client_id) <> << byte_size(consumer_group) :: 16, consumer_group :: binary >>
  end

  @spec parse_response(binary) :: Response.t
  def parse_response(<< _corr_id :: 32, error_code :: 16, coord_id :: 32, coord_host_size :: 16, coord_host :: size(coord_host_size)-binary, coord_port :: 32, rest :: binary >>) do
    %Response{coordinator_id: coord_id, coordinator_host: coord_host, coordinator_port: coord_port, error_code: error_code}
  end
end
