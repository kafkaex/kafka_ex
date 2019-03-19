defmodule KafkaEx.Protocol.OffsetCommit.V1 do
  alias KafkaEx.Protocol
  alias KafkaEx.Protocol.OffsetCommit, as: V0

  @moduledoc """
  Implementation of the Kafka OffsetCommit request and response APIs
  """

  defmodule Request do
    @moduledoc false
    defstruct consumer_group: nil,
              topic:          nil,
              partition:      nil,
              offset:         nil,
              metadata:       "",
              member_id:      "",
              timestamp:      -1,
              generation_id:  -1,
              api_version:    1

    @type t :: %Request{
            consumer_group: binary,
            topic:          binary,
            partition:      integer,
            offset:         integer
          }
  end

  # Taken from https://kafka.apache.org/protocol#The_Messages_OffsetCommit
  @spec create_request(integer, binary, Request.t()) :: binary
  def create_request(correlation_id, client_id, offset_commit_request) do
    Protocol.create_request(:offset_commit, correlation_id, client_id, 1) <>
      <<byte_size(offset_commit_request.consumer_group)::16-signed,
        offset_commit_request.consumer_group::binary,
        offset_commit_request.generation_id::32-signed,
        byte_size(offset_commit_request.member_id)::16-signed,
        offset_commit_request.member_id::binary,
        1::32-signed, # Array count, hence the indentation
          byte_size(offset_commit_request.topic)::16-signed,
          offset_commit_request.topic::binary,
          1::32-signed,
            offset_commit_request.partition::32-signed,
            offset_commit_request.offset::64,
            offset_commit_request.timestamp::64,
            byte_size(offset_commit_request.metadata)::16-signed,
            offset_commit_request.metadata::binary>>
  end

  defdelegate parse_response(response), to: V0
end
