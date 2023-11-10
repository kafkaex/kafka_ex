defmodule KafkaEx.Protocol.Heartbeat do
  @moduledoc """
  Implementation of the Kafka Heartbeat request and response APIs
  """

  defmodule Request do
    @moduledoc false
    defstruct group_name: nil, member_id: nil, generation_id: nil

    @type t :: %Request{
            group_name: binary,
            member_id: binary,
            generation_id: integer
          }
  end

  defmodule Response do
    @moduledoc false
    # We could just return the error code instead of having the struct, but this
    # keeps the code normalized
    defstruct error_code: nil
    @type t :: %Response{error_code: atom | integer} | {:error, atom}
  end

  @spec create_request(integer, binary, Request.t()) :: iodata
  def create_request(correlation_id, client_id, request) do
    [
      KafkaEx.Protocol.create_request(:heartbeat, correlation_id, client_id),
      <<byte_size(request.group_name)::16-signed, request.group_name::binary,
        request.generation_id::32-signed, byte_size(request.member_id)::16-signed,
        request.member_id::binary>>
    ]
  end

  @spec parse_response(binary) :: Response.t()
  def parse_response(<<_correlation_id::32-signed, error_code::16-signed>>) do
    %Response{error_code: KafkaEx.Protocol.error(error_code)}
  end
end
