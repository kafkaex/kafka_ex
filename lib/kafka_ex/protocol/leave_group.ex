defmodule KafkaEx.Protocol.LeaveGroup do
  defmodule Response do
    defstruct error_code: nil
  end

  def create_request(correlation_id, client_id, group_id, member_id) do
    KafkaEx.Protocol.create_request(:leave_group, correlation_id, client_id) <>
      << byte_size(group_id) :: 16-signed, group_id :: binary,
         byte_size(member_id) :: 16-signed, member_id :: binary >>
  end

  def parse_response(<< _correlation_id :: 32-signed, error_code :: 16-signed >>) do
    %Response{error_code: KafkaEx.Protocol.error(error_code)}
  end
end
