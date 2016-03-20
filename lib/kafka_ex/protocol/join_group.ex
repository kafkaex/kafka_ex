defmodule KafkaEx.Protocol.JoinGroup do
  import KafkaEx.Protocol.Common

  @protocol_type "consumer"
  @strategy_name "assign"
  @metadata_version 0

  defmodule Response do
    defstruct error_code: 0, generation_id: 0, leader: "", members: []
    @type t :: %Response{error_code: integer, generation_id: integer, leader: binary, members: list}
  end

  @spec create_request(integer, binary, binary, binary, list, integer) :: binary
  def create_request(correlation_id, client_id, member_id, group_name, topics, session_timeout) do
    KafkaEx.Protocol.create_request(:join_group, correlation_id, client_id) <>
      << byte_size(group_name) :: 16-signed, group_name :: binary,
         session_timeout :: 32-signed,
         byte_size(member_id) :: 16-signed, member_id :: binary,
         byte_size(@protocol_type) :: 16-signed, @protocol_type :: binary,
         1 :: 32-signed, # We always have just one GroupProtocl
         byte_size(@strategy_name) :: 16-signed, @strategy_name :: binary,
         @metadata_version :: 16-signed,
         length(topics) :: 32-signed, topic_data(topics) :: binary,
         0 :: 32-signed
         >>
  end

  @spec parse_response(binary) :: Response.t
  def parse_response(<< _correlation_id :: 32-signed, error_code :: 16-signed, generation_id :: 32-signed,
                       protocol_len :: 16-signed, _protocol :: size(protocol_len)-binary,
                       leader_len :: 16-signed, leader :: size(leader_len)-binary,
                       member_id_len :: 16-signed, _member_id :: size(member_id_len)-binary,
                       members_size :: 32-signed, rest :: binary >>) do
    members = parse_members(members_size, rest, [])
    %Response{error_code: KafkaEx.Protocol.error(error_code), generation_id: generation_id,
              leader: leader, members: members}
  end

  defp parse_members(0, _rest, members), do: members
  defp parse_members(size, << member_len :: 16-signed, member :: size(member_len)-binary, rest :: binary >>, members) do
    parse_members(size - 1, rest, [member|members])
  end
end
