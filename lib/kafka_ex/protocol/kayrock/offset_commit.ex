defmodule KafkaEx.Protocol.Kayrock.OffsetCommit do
  @moduledoc """
  This module handles Offset Commit request & response parsing.
  Request is built using Kayrock protocol, response is parsed to
  native KafkaEx structs.

  The OffsetCommit API commits consumer group offsets to Kafka.
  It supports multiple versions:
  - **v0**: Basic commit to Zookeeper
  - **v1**: Adds timestamp, generation_id, member_id (Kafka/ZK storage)
  - **v2**: Removes timestamp, adds retention_time (Kafka storage, recommended)
  - **v3**: Includes throttle time
  - **v4**: Same as V3 (pure version bump)
  - **v5**: Removes retention_time_ms from request
  - **v6**: Adds committed_leader_epoch per partition
  - **v7**: Adds group_instance_id for static membership (KIP-345)
  - **v8**: Flexible version with compact encodings + tagged_fields (KIP-482)
  - **v9**: First version usable with the KIP-848 consumer protocol;
    `generation_id` becomes `generation_id_or_member_epoch` (fed from
    `:member_epoch`, else `:generation_id`); can return `STALE_MEMBER_EPOCH`
    and `GROUP_ID_NOT_FOUND`
  - **v10**: Topics identified by `topic_id` instead of name (KIP-848); used
    only when the ids of all committed topics are known from cluster metadata,
    otherwise v9 is sent

  All versions V0-V10 have explicit implementations; the `Any` fallback is
  retained only for forward compatibility.
  """

  defprotocol Request do
    @moduledoc """
    This protocol is used to build Offset Commit requests
    """
    @fallback_to_any true
    @spec build_request(t(), Keyword.t()) :: t()
    def build_request(request, opts)
  end

  defprotocol Response do
    @moduledoc """
    This protocol is used to parse Offset Commit responses
    """
    @fallback_to_any true
    alias KafkaEx.Messages.Offset

    @spec parse_response(t()) :: {:ok, [Offset.t()]} | {:error, term}
    def parse_response(response)
  end
end
