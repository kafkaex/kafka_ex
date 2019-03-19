defmodule KafkaEx.Protocol.OffsetFetch.V1 do
  alias KafkaEx.Protocol
  alias KafkaEx.Protocol.OffsetFetch, as: V0

  @moduledoc """
  Implementation of the Kafka OffsetFetch request and response APIs
  """

  defmodule Request do
    @moduledoc false
    defstruct consumer_group: nil,
              topic:          nil,
              partition:      nil,
              api_version:    1

    @type t :: %Request{
            consumer_group: nil | binary,
            topic:          binary,
            partition:      integer
          }
  end

  # Version 0 and Version 1 don't differ, so that all that is done here is
  # to update the api version to 1.
  #
  # However a Version 1 of OffsetFetch is required because offsets
  # committed using Version 1 can only be fetched by a version 1 request.
  def create_request(correlation_id, client_id, offset_fetch_request) do
    Protocol.create_request(:offset_fetch, correlation_id,
      client_id, 1) <>
        <<byte_size(offset_fetch_request.consumer_group)::16-signed,
        offset_fetch_request.consumer_group::binary,
        1::32-signed, # Array count, hence the indentation
          byte_size(offset_fetch_request.topic)::16-signed,
          offset_fetch_request.topic::binary,
          1::32-signed,
            offset_fetch_request.partition::32>>
  end

  defdelegate parse_response(response), to: V0
end
