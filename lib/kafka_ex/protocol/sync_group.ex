defmodule KafkaEx.Protocol.SyncGroup do
  @moduledoc """
  Implementation of the Kafka SyncGroup request and response APIs
  """
  @member_assignment_version 0

  defmodule Request do
    @moduledoc false
    defstruct member_id: nil,
              group_name: nil,
              generation_id: nil,
              assignments: []

    @type t :: %Request{
            member_id: binary,
            group_name: binary,
            generation_id: integer,
            assignments: [
              {member :: binary, [{topic :: binary, partitions :: [integer]}]}
            ]
          }
  end

  defmodule Assignment do
    @moduledoc false
    defstruct topic: nil, partitions: []
    @type t :: %Assignment{topic: binary, partitions: [integer]}
  end

  defmodule Response do
    @moduledoc false
    defstruct error_code: nil, assignments: []

    @type t :: %Response{
            error_code: atom | integer,
            assignments: [Assignment.t()]
          }
  end

  @spec create_request(integer, binary, Request.t()) :: iodata
  def create_request(correlation_id, client_id, %Request{} = request) do
    [
      KafkaEx.Protocol.create_request(:sync_group, correlation_id, client_id),
      <<byte_size(request.group_name)::16-signed, request.group_name::binary,
        request.generation_id::32-signed, byte_size(request.member_id)::16-signed,
        request.member_id::binary, length(request.assignments)::32-signed,
        group_assignment_data(request.assignments, "")::binary>>
    ]
  end

  @spec parse_response(binary) :: Response.t()
  def parse_response(
        <<_correlation_id::32-signed, error_code::16-signed, member_assignment_len::32-signed,
          member_assignment::size(member_assignment_len)-binary>>
      ) do
    %Response{
      error_code: KafkaEx.Protocol.error(error_code),
      assignments: parse_member_assignment(member_assignment)
    }
  end

  # Helper functions to create assignment data structure

  defp group_assignment_data([], acc), do: acc

  defp group_assignment_data([h | t], acc),
    do: group_assignment_data(t, acc <> member_assignment_data(h))

  defp member_assignment_data({member_id, member_assignment}) do
    assignment_bytes_for_member = <<
      @member_assignment_version::16-signed,
      length(member_assignment)::32-signed,
      topic_assignment_data(member_assignment, "")::binary,
      # UserData
      0::32-signed
    >>

    <<byte_size(member_id)::16-signed, member_id::binary,
      byte_size(assignment_bytes_for_member)::32-signed, assignment_bytes_for_member::binary>>
  end

  defp topic_assignment_data([], acc), do: acc

  defp topic_assignment_data([h | t], acc),
    do: topic_assignment_data(t, acc <> partition_assignment_data(h))

  defp partition_assignment_data({topic_name, partition_ids}) do
    <<byte_size(topic_name)::16-signed, topic_name::binary, length(partition_ids)::32-signed,
      partition_id_data(partition_ids, "")::binary>>
  end

  defp partition_id_data([], acc), do: acc

  defp partition_id_data([h | t], acc),
    do: partition_id_data(t, acc <> <<h::32-signed>>)

  # Helper functions to parse assignments

  defp parse_member_assignment(<<>>), do: []

  defp parse_member_assignment(
         <<@member_assignment_version::16-signed, assignments_size::32-signed, rest::binary>>
       ) do
    parse_assignments(assignments_size, rest, [])
  end

  defp parse_assignments(0, _rest, assignments), do: assignments

  defp parse_assignments(
         size,
         <<topic_len::16-signed, topic::size(topic_len)-binary, partition_len::32-signed,
           rest::binary>>,
         assignments
       ) do
    {partitions, rest} = parse_partitions(partition_len, rest, [])
    parse_assignments(size - 1, rest, [{topic, partitions} | assignments])
  end

  defp parse_partitions(0, rest, partitions), do: {partitions, rest}

  defp parse_partitions(
         size,
         <<partition::32-signed, rest::binary>>,
         partitions
       ) do
    parse_partitions(size - 1, rest, [partition | partitions])
  end
end
