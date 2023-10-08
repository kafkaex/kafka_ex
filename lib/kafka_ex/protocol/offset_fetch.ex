defmodule KafkaEx.Protocol.OffsetFetch do
  alias KafkaEx.Protocol
  import KafkaEx.Protocol.Common

  @moduledoc """
  Implementation of the Kafka OffsetFetch request and response APIs
  """

  defmodule Request do
    @moduledoc false
    defstruct consumer_group: nil,
              topic: nil,
              partition: nil,
              # NOTE api_version only used in new client
              api_version: 0

    @type t :: %Request{
            consumer_group: nil | binary,
            topic: binary,
            partition: integer,
            api_version: integer
          }
  end

  defmodule Response do
    @moduledoc false
    defstruct topic: nil, partitions: []
    @type t :: %Response{topic: binary, partitions: list}

    def last_offset(:topic_not_found) do
      0
    end

    def last_offset(offset_fetch_data) do
      case offset_fetch_data do
        [] ->
          0

        _ ->
          partitions = offset_fetch_data |> hd |> Map.get(:partitions, [])

          case partitions do
            [] -> 0
            _ -> partitions |> hd |> Map.get(:offset, 0)
          end
      end
    end
  end

  @spec create_request(integer, binary, Request.t()) :: iodata
  def create_request(correlation_id, client_id, offset_fetch_request) do
    [
      KafkaEx.Protocol.create_request(:offset_fetch, correlation_id, client_id),
      <<byte_size(offset_fetch_request.consumer_group)::16-signed,
        offset_fetch_request.consumer_group::binary, 1::32-signed,
        byte_size(offset_fetch_request.topic)::16-signed, offset_fetch_request.topic::binary,
        1::32-signed, offset_fetch_request.partition::32>>
    ]
  end

  def parse_response(<<_correlation_id::32-signed, topics_size::32-signed, topics_data::binary>>) do
    parse_topics(topics_size, topics_data, __MODULE__)
  end

  def parse_partitions(0, rest, partitions, _topic), do: {partitions, rest}

  def parse_partitions(
        partitions_size,
        <<partition::32-signed, offset::64-signed, metadata_size::16-signed,
          metadata::size(metadata_size)-binary, error_code::16-signed, rest::binary>>,
        partitions,
        topic
      ) do
    parse_partitions(
      partitions_size - 1,
      rest,
      [
        %{
          partition: partition,
          offset: offset,
          metadata: metadata,
          error_code: Protocol.error(error_code)
        }
        | partitions
      ],
      topic
    )
  end
end
