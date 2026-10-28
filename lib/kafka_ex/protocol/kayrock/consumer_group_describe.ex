defmodule KafkaEx.Protocol.Kayrock.ConsumerGroupDescribe do
  @moduledoc """
  This module handles ConsumerGroupDescribe (API key 69, KIP-848) request &
  response parsing. Request is built using Kayrock protocol, response is parsed
  to native KafkaEx structs.

  ConsumerGroupDescribe describes groups using the "consumer" group protocol
  (`group.protocol=consumer`, Kafka 4.0+). Classic groups are described with
  DescribeGroups (`KafkaEx.Protocol.Kayrock.DescribeGroups`); asking
  ConsumerGroupDescribe about one returns `:group_id_not_found`.

  ## Supported Versions

  - **V0**: Kafka 3.7+ (KIP-848)
    - Request: `group_ids`, `include_authorized_operations`
    - Response: `groups` (error_code, error_message, group_id, group_state,
      group_epoch, assignment_epoch, assignor_name, members, authorized_operations);
      each member has member_id, instance_id, rack_id, member_epoch, client_id,
      client_host, subscribed_topic_names, subscribed_topic_regex, assignment and
      target_assignment (by topic id and name)
  - **V1**: Kafka 4.0+ (KIP-1099)
    - Request: Same as V0
    - Response: +`member_type` per member

  Both versions are flexible (compact encoding, tagged fields).

  All known versions (V0-V1) have explicit `defimpl` implementations.
  An `Any` fallback is retained for forward compatibility with unknown
  future Kayrock versions.
  """

  defprotocol Request do
    @moduledoc """
    This protocol is used to build ConsumerGroupDescribe requests.

    ## Options (V0-V1)
    - `group_ids` (required): List of consumer group IDs to describe
    - `include_authorized_operations` (optional): defaults to `false`

    All known versions (V0-V1) have explicit implementations.
    The `Any` fallback handles unknown future versions.
    """
    @fallback_to_any true

    @spec build_request(t(), Keyword.t()) :: t()
    def build_request(request, opts)
  end

  defprotocol Response do
    @moduledoc """
    This protocol is used to parse ConsumerGroupDescribe responses.

    ## Return Values
    - Success: `{:ok, [ConsumerGroupDescription.t()]}`
    - Error: `{:error, [{group_id, error_atom}]}` when any group failed, the
      same shape as DescribeGroups

    All known versions (V0-V1) have explicit implementations.
    The `Any` fallback handles unknown future versions.
    """
    @fallback_to_any true

    alias KafkaEx.Messages.ConsumerGroupDescription

    @spec parse_response(t()) :: {:ok, [ConsumerGroupDescription.t()]} | {:error, [{String.t(), atom()}]}
    def parse_response(response)
  end
end
