defmodule KafkaEx.Protocol.LeaveGroup do
  defmodule Request do
    @moduledoc false
    defstruct group_name: nil, member_id: nil

    @type t :: %Request{
            group_name: binary,
            member_id: binary
          }
  end

  defmodule Response do
    @moduledoc false

    defstruct error_code: nil

    @type t ::
            %Response{
              error_code: atom | integer
            }
            | {:error, atom}
  end

  @spec create_request(integer, binary, Request.t()) :: iodata
  def create_request(correlation_id, client_id, request) do
    [
      KafkaEx.Protocol.create_request(:leave_group, correlation_id, client_id),
      <<byte_size(request.group_name)::16-signed, request.group_name::binary,
        byte_size(request.member_id)::16-signed, request.member_id::binary>>
    ]
  end

  def parse_response(<<_correlation_id::32-signed, error_code::16-signed>>) do
    %Response{error_code: KafkaEx.Protocol.error(error_code)}
  end
end
