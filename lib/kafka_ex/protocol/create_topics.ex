
defmodule KafkaEx.Protocol.CreateTopics do
  alias KafkaEx.Protocol

  @moduledoc """
  Implementation of the Kafka CreateTopics request and response APIs
  """

  # CreateTopics Request (Version: 0) => [create_topic_requests] timeout
  # create_topic_requests => topic num_partitions replication_factor [replica_assignment] [config_entries]
  #   topic => STRING
  #   num_partitions => INT32
  #   replication_factor => INT16
  #   replica_assignment => partition [replicas]
  #     partition => INT32
  #     replicas => INT32
  #   config_entries => config_name config_value
  #     config_name => STRING
  #     config_value => NULLABLE_STRING
  # timeout => INT32

  defmodule ReplicaAssignment do
    defstruct partition: nil, replicas: nil
    @type t :: %ReplicaAssignment{ partition: integer, replicas: [integer] }
  end

  defmodule ConfigEntry do
    defstruct config_name: nil, config_value: nil
    @type t :: %ConfigEntry{ config_name: binary, config_value: binary }
  end

  defmodule TopicRequest do
    defstruct topic: nil,
              num_partitions: -1,
              replication_factor: -1,
              replica_assignment: [],
              config_entries: []
    @type t :: %TopicRequest{
      topic: binary,
      num_partitions: integer,
      replication_factor: integer,
      replica_assignment: [ReplicaAssignment],
      config_entries: [ConfigEntry],
    }
  end

  defmodule Request do
    @moduledoc false
    defstruct create_topic_requests: nil, timeout: nil
    @type t :: %Request{create_topic_requests: [TopicRequest], timeout: integer}
  end

  defmodule TopicError do
    defstruct topic_name: nil, error_code: nil
    @type t :: %TopicError{ topic_name: binary, error_code: integer }
  end

  defmodule Response do
    @moduledoc false
    defstruct topic_errors: nil
    @type t :: %Response{topic_errors: [TopicError]}
  end

  @spec create_request(integer, binary, Request.t) :: binary
  def create_request(correlation_id, client_id, create_topics_request) do
    Protocol.create_request(:create_topics, correlation_id, client_id) <>
      encode_topic_requests(create_topics_request.create_topic_requests) <>
        << create_topics_request.timeout  :: 32-signed >>
  end

  @spec encode_topic_requests([TopicRequest.t]) :: binary
  defp encode_topic_requests(requests) do
    requests
    |> map_encode(&encode_topic_request/1)
  end

  @spec encode_topic_request(TopicRequest.t) :: binary
  defp encode_topic_request(request) do
    encode_string(request.topic) <>
      << request.num_partitions :: 32-signed, request.replication_factor :: 16-signed >> <>
      encode_replica_assignments(request.replica_assignment) <>
      encode_config_entries(request.config_entries)
  end


  @spec encode_replica_assignments([ReplicaAssignment.t]) :: binary
  defp encode_replica_assignments(replica_assignments) do
    replica_assignments |> map_encode(&encode_replica_assignment/1)
  end

  defp encode_replica_assignment(replica_assignment) do
    << replica_assignment.partition :: 32-signed >> <>
    replica_assignment.replicas |> map_encode(&(<< &1 :: 32-signed >>))
  end

  @spec encode_config_entries([ConfigEntry.t]) :: binary
  defp encode_config_entries(config_entries) do
    config_entries |> map_encode(&encode_config_entry/1)
  end

  @spec encode_config_entry(ConfigEntry.t) :: binary
  defp encode_config_entry(config_entry) do
    encode_string(config_entry.config_name) <> encode_nullable_string(config_entry.config_value)
  end

  @spec encode_nullable_string(String.t) :: binary
  defp encode_nullable_string(text) do
    if text == nil do
      << -1 :: 16-signed >>
    else
      encode_string(text)
    end
  end

  @spec encode_string(String.t) :: binary
  defp encode_string(text) do
    << byte_size(text) :: 16-signed, text :: binary, >>
  end

  defp map_encode(elems, function) do
    if nil == elems or 0 == length(elems) do
      << 0 ::  32-signed >>
    else
      << length(elems) ::  32-signed >> <>
      (elems
        |> Enum.map(function)
        |> Enum.reduce(&(&1 <> &2)))
    end

  end

  @spec parse_response(binary) :: [] | Response.t
  def parse_response(<< _correlation_id :: 32-signed, topic_errors_count :: 32-signed, topic_errors :: binary >>) do
    %Response{topic_errors: parse_topic_errors(topic_errors_count, topic_errors)}
  end

  defp parse_topic_errors(0, _), do: []

  @spec parse_topic_errors(integer, binary) :: [TopicError.t]
  defp parse_topic_errors(topic_errors_count,
      << topic_name_size :: 16-signed, topic_name :: size(topic_name_size)-binary, error_code :: 16-signed, rest :: binary >>) do
    [%TopicError{topic_name: topic_name, error_code: error_code} | parse_topic_errors(topic_errors_count - 1, rest)]
  end

end
