defmodule KafkaEx.Protocol.ConsumerMetadata do
  alias KafkaEx.Protocol
  alias KafkaEx.Socket

  @moduledoc """
  Implementation of the Kafka ConsumerMetadata request and response APIs
  """

  defmodule Response do
    @moduledoc false
    defstruct coordinator_id: 0, coordinator_host: "", coordinator_port: 0, error_code: 0
    @type t :: %Response{
      coordinator_id: integer,
      coordinator_host: binary,
      coordinator_port: 0..65_535,
      error_code: atom
    }

    def broker_for_consumer_group(brokers, consumer_group_metadata) do
      Enum.find(brokers, &(&1.node_id == consumer_group_metadata.coordinator_id && &1.socket && is_list(Socket.info(&1.socket))))
    end
  end

  # these complain of binary a underspec that can't be fixed for elixir < 1.3
  @dialyzer [
    {:nowarn_function, create_request: 3}
  ]

  @spec create_request(integer, binary, binary) :: binary
  def create_request(correlation_id, client_id, consumer_group) do
    KafkaEx.Protocol.create_request(:consumer_metadata, correlation_id, client_id) <> << byte_size(consumer_group) :: 16-signed, consumer_group :: binary >>
  end

  @spec parse_response(binary) :: Response.t
  def parse_response(<< _corr_id :: 32-signed, error_code :: 16-signed, coord_id :: 32-signed, coord_host_size :: 16-signed, coord_host :: size(coord_host_size)-binary, coord_port :: 32-signed, _ :: binary >>) do
    %Response{coordinator_id: coord_id, coordinator_host: coord_host, coordinator_port: coord_port, error_code: Protocol.error(error_code)}
  end
end
