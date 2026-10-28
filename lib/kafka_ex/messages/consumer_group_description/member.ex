defmodule KafkaEx.Messages.ConsumerGroupDescription.Member do
  @moduledoc """
  Information about a member of a consumer group.

  Java equivalent: `org.apache.kafka.clients.admin.MemberDescription`

  Contains identifying information about a consumer group member and their
  current partition assignment.

  ## Fields set by ConsumerGroupDescribe only

  Members of a KIP-848 "consumer" protocol group (`KafkaEx.API.describe_consumer_group/3`) have no
  `member_metadata` (it is `nil`); their `member_assignment` is the current assignment (its
  `PartitionAssignment`s carry both `topic` and `topic_id`) and `group_instance_id` is the static
  instance id. They also carry the following fields, which are `nil` in DescribeGroups results:

    * `member_epoch` - the member's current epoch
    * `rack_id` - the member's rack id, if any
    * `subscribed_topic_names` - topic names the member subscribed to
    * `subscribed_topic_regex` - the member's subscription regex, if any
    * `target_assignment` - the assignment the member is converging to (`MemberAssignment`)
    * `member_type` - `:consumer`, `:classic` (a classic-protocol member of an upgraded
      group) or `:unknown`; ConsumerGroupDescribe v1+ only, `nil` on v0
  """
  alias KafkaEx.Messages.ConsumerGroupDescription.Member.MemberAssignment
  alias KafkaEx.Messages.ConsumerGroupDescription.Member.MemberAssignment.PartitionAssignment

  @type t :: %__MODULE__{
          member_id: binary,
          client_id: binary,
          client_host: binary,
          member_metadata: term,
          member_assignment: MemberAssignment.t(),
          group_instance_id: binary | nil,
          member_epoch: integer() | nil,
          rack_id: binary | nil,
          subscribed_topic_names: [binary] | nil,
          subscribed_topic_regex: binary | nil,
          target_assignment: MemberAssignment.t() | nil,
          member_type: :consumer | :classic | :unknown | nil
        }

  defstruct [
    :member_id,
    :client_id,
    :client_host,
    :member_metadata,
    :group_instance_id,
    :member_epoch,
    :rack_id,
    :subscribed_topic_names,
    :subscribed_topic_regex,
    :target_assignment,
    :member_type,
    member_assignment: %MemberAssignment{}
  ]

  @type partial_response :: %{
          required(:member_id) => binary,
          required(:client_id) => binary,
          required(:client_host) => binary,
          required(:member_metadata) => term,
          optional(:member_assignment) => map | binary | nil
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
      member_assignment: build_member_assignment(Map.get(response, :member_assignment)),
      group_instance_id: Map.get(response, :group_instance_id)
    }
  end

  @doc """
  Builds a Member from one member of a ConsumerGroupDescribe API response (KIP-848).
  """
  @spec from_consumer_group_describe_response(map) :: __MODULE__.t()
  def from_consumer_group_describe_response(member) do
    %__MODULE__{
      member_id: member.member_id,
      client_id: member.client_id,
      client_host: member.client_host,
      member_metadata: nil,
      member_assignment: MemberAssignment.from_topic_partitions(member.assignment),
      group_instance_id: member.instance_id,
      member_epoch: member.member_epoch,
      rack_id: member.rack_id,
      subscribed_topic_names: member.subscribed_topic_names,
      subscribed_topic_regex: member.subscribed_topic_regex,
      target_assignment: MemberAssignment.from_topic_partitions(member.target_assignment),
      member_type: member_type(Map.get(member, :member_type))
    }
  end

  # ConsumerGroupDescribe v1+ (KIP-1099): -1 unknown, 0 classic, 1 consumer.
  defp member_type(nil), do: nil
  defp member_type(0), do: :classic
  defp member_type(1), do: :consumer
  defp member_type(_), do: :unknown

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
  @spec assignment(t()) :: MemberAssignment.t()
  def assignment(%__MODULE__{member_assignment: assignment}), do: assignment

  @empty_assignment %MemberAssignment{}

  defp build_member_assignment(nil), do: @empty_assignment
  defp build_member_assignment(""), do: @empty_assignment

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
