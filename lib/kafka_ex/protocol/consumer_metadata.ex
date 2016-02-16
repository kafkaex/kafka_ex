defmodule KafkaEx.Protocol.ConsumerMetadata do
  defmodule Response do
    defstruct coordinator_id: nil, coordinator_host: nil, coordinator_port: nil, error_code: 0
    @type t :: %Response{coordinator_id: integer, coordinator_host: binary, coordinator_port: 0..65535, error_code: integer}

    def broker_for_consumer_group(brokers, consumer_group_metadata) do
      Enum.find(brokers, &(&1.host == consumer_group_metadata.coordinator_host && &1.port == consumer_group_metadata.coordinator_port && &1.socket && is_list(Port.info(&1.socket))))
    end
  end

  @spec create_request(integer, binary, binary) :: binary
  def create_request(correlation_id, client_id, consumer_group)
  when is_integer(correlation_id) and correlation_id >= 0
  and is_binary(client_id) and byte_size(client_id) > 0
  and is_binary(consumer_group) and byte_size(consumer_group) > 0 do
    KafkaEx.Protocol.create_request(:consumer_metadata, correlation_id, client_id) <> << byte_size(consumer_group) :: 16-signed, consumer_group :: binary >>
  end

  @spec parse_response(binary) :: Response.t
  def parse_response(<< _corr_id :: 32-signed, error_code :: 16-signed, coord_id :: 32-signed, coord_host_size :: 16-signed, coord_host :: size(coord_host_size)-binary, coord_port :: 32-signed, _ :: binary >>) do
    %Response{coordinator_id: coord_id, coordinator_host: coord_host, coordinator_port: coord_port, error_code: error_code}
  end
end
