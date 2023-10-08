defmodule KafkaEx.Protocol.DeleteTopics do
  alias KafkaEx.Protocol
  import KafkaEx.Protocol.Common
  @supported_versions_range {0, 0}

  @moduledoc """
  Implementation of the Kafka DeleteTopics request and response APIs

  See: https://kafka.apache.org/protocol.html#The_Messages_DeleteTopics
  """

  # DeleteTopics Request (Version: 0) => [topics] timeout
  #   topics => STRING
  #   timeout => INT32

  # DeleteTopics Response (Version: 0) => [topic_error_codes]
  #   topic_error_codes => topic error_code
  #     topic => STRING
  #     error_code => INT16

  defmodule Request do
    @moduledoc false
    defstruct topics: nil, timeout: nil
    @type t :: %Request{topics: [String.t()], timeout: integer}
  end

  defmodule TopicError do
    @moduledoc false
    defstruct topic_name: nil, error_code: nil
    @type t :: %TopicError{topic_name: binary, error_code: atom}
  end

  defmodule Response do
    @moduledoc false
    defstruct topic_errors: nil
    @type t :: %Response{topic_errors: [TopicError.t()]}
  end

  def api_version(api_versions) do
    KafkaEx.ApiVersions.find_api_version(
      api_versions,
      :delete_topics,
      @supported_versions_range
    )
  end

  @spec create_request(integer, binary, Request.t(), integer) :: iodata
  def create_request(
        correlation_id,
        client_id,
        delete_topics_request,
        api_version
      )

  def create_request(correlation_id, client_id, delete_topics_request, 0) do
    [
      Protocol.create_request(:delete_topics, correlation_id, client_id),
      encode_topics(delete_topics_request.topics),
      <<delete_topics_request.timeout::32-signed>>
    ]
  end

  @spec encode_topics([String.t()]) :: binary
  defp encode_topics(topics) do
    topics |> map_encode(&encode_string/1)
  end

  @spec parse_response(binary, integer) :: [] | Response.t()
  def parse_response(
        <<_correlation_id::32-signed, topic_errors_count::32-signed, topic_errors::binary>>,
        0
      ) do
    %Response{
      topic_errors: parse_topic_errors(topic_errors_count, topic_errors)
    }
  end

  @spec parse_topic_errors(integer, binary) :: [TopicError.t()]
  defp parse_topic_errors(0, _), do: []

  defp parse_topic_errors(
         topic_errors_count,
         <<topic_name_size::16-signed, topic_name::size(topic_name_size)-binary,
           error_code::16-signed, rest::binary>>
       ) do
    [
      %TopicError{
        topic_name: topic_name,
        error_code: Protocol.error(error_code)
      }
      | parse_topic_errors(topic_errors_count - 1, rest)
    ]
  end
end
