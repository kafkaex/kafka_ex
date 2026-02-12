defmodule KafkaEx.Messages.ConsumerGroupDescription.Member do
  @moduledoc """
  Information about a member of a consumer group.

  Java equivalent: `org.apache.kafka.clients.admin.MemberDescription`

  Contains identifying information about a consumer group member and their
  current partition assignment.
  """
  alias KafkaEx.Messages.ConsumerGroupDescription.Member.MemberAssignment
  alias KafkaEx.Messages.ConsumerGroupDescription.Member.MemberAssignment.PartitionAssignment

  @type t :: %__MODULE__{
          member_id: binary,
          client_id: binary,
          client_host: binary,
          member_metadata: term,
          member_assignment: MemberAssignment.t() | nil
        }

  defstruct ~w(member_id client_id client_host member_metadata member_assignment)a

  @type partial_response :: %{
          required(:member_id) => binary,
          required(:client_id) => binary,
          required(:client_host) => binary,
          required(:member_metadata) => term,
          optional(:member_assignment) => map | nil
        }

  @doc """
  Builds a Member from a DescribeGroups API response.
  """
  @spec from_describe_group_response(partial_response()) :: __MODULE__.t()
  def from_describe_group_response(response) do
    %__MODULE__{
      member_id: response.member_id,
      client_id: response.client_id,
      client_host: response.client_host,
      member_metadata: response.member_metadata,
      member_assignment: build_member_assignment(Map.get(response, :member_assignment))
    }
  end

  @doc """
  Returns the member ID.
  """
  @spec member_id(t()) :: binary
  def member_id(%__MODULE__{member_id: member_id}), do: member_id

  @doc """
  Returns the client ID.
  """
  @spec client_id(t()) :: binary
  def client_id(%__MODULE__{client_id: client_id}), do: client_id

  @doc """
  Returns the client host.
  """
  @spec client_host(t()) :: binary
  def client_host(%__MODULE__{client_host: client_host}), do: client_host

  @doc """
  Returns the member's current partition assignment.
  """
  @spec assignment(t()) :: MemberAssignment.t() | nil
  def assignment(%__MODULE__{member_assignment: assignment}), do: assignment

  defp build_member_assignment(nil), do: nil
  defp build_member_assignment(""), do: nil

  defp build_member_assignment(binary) when is_binary(binary) do
    deserialize_assignment(binary)
  end

  defp build_member_assignment(member_assignment) when is_map(member_assignment) do
    MemberAssignment.from_describe_group_response(member_assignment)
  end

  # Deserializes ConsumerProtocol Assignment binary format:
  # version:int16, topic_partitions:array[topic:string16, partitions:array[int32]], user_data:bytes
  defp deserialize_assignment(<<version::16-signed, rest::binary>>) do
    {topic_partitions, rest} = deserialize_array(rest, &deserialize_topic_partition/1)
    {user_data, _rest} = deserialize_bytes(rest)

    partition_assignments =
      Enum.map(topic_partitions, fn {topic, partitions} ->
        PartitionAssignment.from_describe_group_response(%{
          topic: topic,
          partitions: partitions
        })
      end)

    %MemberAssignment{
      version: version,
      user_data: user_data || <<>>,
      partition_assignments: partition_assignments
    }
  end

  defp deserialize_topic_partition(<<topic_len::16-signed, topic::binary-size(topic_len), rest::binary>>) do
    {partitions, rest} = deserialize_array(rest, &deserialize_int32/1)
    {{topic, partitions}, rest}
  end

  defp deserialize_int32(<<value::32-signed, rest::binary>>), do: {value, rest}

  defp deserialize_array(<<count::32-signed, rest::binary>>, deserializer) when count >= 0 do
    {items, rest} =
      Enum.reduce(1..count//1, {[], rest}, fn _, {acc, data} ->
        {item, remaining} = deserializer.(data)
        {[item | acc], remaining}
      end)

    {Enum.reverse(items), rest}
  end

  defp deserialize_array(<<_negative::32-signed, rest::binary>>, _deserializer), do: {[], rest}

  defp deserialize_bytes(<<length::32-signed, data::binary-size(length), rest::binary>>)
       when length >= 0,
       do: {data, rest}

  defp deserialize_bytes(<<_negative::32-signed, rest::binary>>), do: {nil, rest}
  defp deserialize_bytes(<<>>), do: {nil, <<>>}
end
