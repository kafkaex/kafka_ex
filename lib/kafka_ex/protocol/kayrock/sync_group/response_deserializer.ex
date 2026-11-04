defmodule KafkaEx.Protocol.Kayrock.SyncGroup.ResponseDeserializer do
  @moduledoc """
  Wire decoder for SyncGroup v0-v4 responses, used instead of Kayrock's
  generated one (see `KafkaEx.Protocol.KayrockProtocol.response_deserializer/1`).

  Kayrock decodes the member assignment (a `ConsumerProtocolAssignment`) with
  a non-nullable `user_data`. In Kafka's schema `userData` is *nullable*
  bytes, and a Kafka 4.x coordinator sends null (length `-1`) when it writes
  the assignment of a classic member of a group that the KIP-848 consumer
  protocol has taken over (online migration: a `group_protocol: :consumer`
  member joined a classic group). Kayrock then raises, the client reports
  `:parse_error`, and the classic member gives up after its SyncGroup
  retries, so a rolling migration could never get past its first step.

  This decoder produces the same `Kayrock.SyncGroup.V*.Response` structs with
  a `Kayrock.MemberAssignment` whose `user_data` is `""` for null. Every
  other field is decoded with Kayrock's own primitives exactly as before.
  """

  alias Kayrock.Deserialize
  alias Kayrock.MemberAssignment
  alias Kayrock.MemberAssignment.PartitionAssignment
  alias Kayrock.SyncGroup

  @doc """
  The deserializer for a SyncGroup request of versions 0-4.
  """
  @spec for_request(struct()) :: (binary() -> {struct(), binary()})
  def for_request(%SyncGroup.V0.Request{}), do: &deserialize_v0/1
  def for_request(%SyncGroup.V1.Request{}), do: &deserialize_v1_v3(&1, SyncGroup.V1.Response)
  def for_request(%SyncGroup.V2.Request{}), do: &deserialize_v1_v3(&1, SyncGroup.V2.Response)
  def for_request(%SyncGroup.V3.Request{}), do: &deserialize_v1_v3(&1, SyncGroup.V3.Response)
  def for_request(%SyncGroup.V4.Request{}), do: &deserialize_v4/1

  @doc false
  @spec deserialize_v0(binary()) :: {struct(), binary()}
  def deserialize_v0(<<correlation_id::32-signed, error_code::16-signed, rest::binary>>) do
    {assignment, rest} = assignment_bytes(rest)
    {%SyncGroup.V0.Response{correlation_id: correlation_id, error_code: error_code, assignment: assignment}, rest}
  end

  @doc false
  @spec deserialize_v1_v3(binary(), module()) :: {struct(), binary()}
  def deserialize_v1_v3(
        <<correlation_id::32-signed, throttle_time_ms::32-signed, error_code::16-signed, rest::binary>>,
        module
      ) do
    {assignment, rest} = assignment_bytes(rest)

    {struct(module,
       correlation_id: correlation_id,
       throttle_time_ms: throttle_time_ms,
       error_code: error_code,
       assignment: assignment
     ), rest}
  end

  @doc false
  @spec deserialize_v4(binary()) :: {struct(), binary()}
  def deserialize_v4(<<correlation_id::32-signed, rest::binary>>) do
    # Response header v1: tagged fields.
    {_header_tags, rest} = Deserialize.deserialize_tagged_fields(rest)
    <<throttle_time_ms::32-signed, error_code::16-signed, rest::binary>> = rest
    {raw, rest} = Deserialize.deserialize(:compact_bytes, rest)
    {tagged_fields, rest} = Deserialize.deserialize_tagged_fields(rest)

    {%SyncGroup.V4.Response{
       correlation_id: correlation_id,
       throttle_time_ms: throttle_time_ms,
       error_code: error_code,
       assignment: content(raw),
       tagged_fields: tagged_fields
     }, rest}
  end

  # v0-v3: int32-length bytes; an empty (0-length) assignment is the empty
  # struct, as in Kayrock.
  defp assignment_bytes(<<size::32-signed, data::binary-size(size), rest::binary>>) when size >= 0,
    do: {content(data), rest}

  @doc """
  Decodes a `ConsumerProtocolAssignment` (version, assigned partitions, and
  nullable `user_data`; extra bytes after it, from newer assignment versions,
  are ignored as Kafka does).
  """
  @spec content(binary() | nil) :: MemberAssignment.t()
  def content(nil), do: %MemberAssignment{}
  def content(<<>>), do: %MemberAssignment{}

  def content(<<version::16-signed, count::32-signed, rest::binary>>) do
    {assignments, rest} = partition_assignments(count, rest, [])
    %MemberAssignment{version: version, partition_assignments: assignments, user_data: user_data(rest)}
  end

  defp partition_assignments(count, rest, acc) when count <= 0, do: {acc, rest}

  defp partition_assignments(count, rest, acc) do
    {topic, rest} = Deserialize.deserialize(:string, rest)
    <<partition_count::32-signed, rest::binary>> = rest
    {partitions, rest} = partitions(partition_count, rest, [])
    partition_assignments(count - 1, rest, [%PartitionAssignment{topic: topic, partitions: partitions} | acc])
  end

  defp partitions(count, rest, acc) when count <= 0, do: {acc, rest}

  defp partitions(count, <<partition::32-signed, rest::binary>>, acc),
    do: partitions(count - 1, rest, [partition | acc])

  defp user_data(<<>>), do: ""
  defp user_data(<<-1::32-signed, _rest::binary>>), do: ""
  defp user_data(<<size::32-signed, data::binary-size(size), _rest::binary>>) when size >= 0, do: data
end
