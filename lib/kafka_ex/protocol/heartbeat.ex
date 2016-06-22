defmodule KafkaEx.Protocol.Heartbeat do

  defmodule Response do
    # We could just return the error code instead of having the struct, but this
    # keeps the code normalized
    defstruct error_code: nil
    @type t :: %Response{error_code: atom | integer}
  end

  @spec create_request(integer, binary, binary, binary, integer) :: <<_::64, _::_ *8>>
  def create_request(correlation_id, client_id, member_id, group_id, generation_id) do
    KafkaEx.Protocol.create_request(:heartbeat, correlation_id, client_id) <>
      << byte_size(group_id) :: 16-signed, group_id :: binary,
         generation_id :: 32-signed,
         byte_size(member_id) :: 16-signed, member_id :: binary >>
  end

  @spec parse_response(<<_::48>>) :: Response.t
  def parse_response(<< _correlation_id :: 32-signed, error_code :: 16-signed >>) do
    %Response{error_code: KafkaEx.Protocol.error(error_code)}
  end

end
