defmodule KafkaEx.Protocol do
  @moduledoc false

  @message_type_to_api_key %{
    produce: 0,
    fetch: 1,
    offset: 2,
    metadata: 3,
    offset_commit: 8,
    offset_fetch: 9,
    consumer_metadata: 10,
    join_group: 11,
    heartbeat: 12,
    leave_group: 13,
    sync_group: 14,
    api_versions: 18,
    create_topics: 19,
    delete_topics: 20
  }

  # DescribeConfigs	32
  # AlterConfigs	33 Valid resource types are "Topic" and "Broker".

  @default_api_version 0

  @spec api_key(atom) :: integer | nil
  def api_key(type) do
    Map.get(@message_type_to_api_key, type, nil)
  end

  def create_request(
        type,
        correlation_id,
        client_id,
        api_version \\ @default_api_version
      ) do
    <<api_key(type)::16, api_version::16, correlation_id::32, byte_size(client_id)::16,
      client_id::binary>>
  end

  @error_map %{
    0 => :no_error,
    1 => :offset_out_of_range,
    2 => :invalid_message,
    3 => :unknown_topic_or_partition,
    4 => :invalid_message_size,
    5 => :leader_not_available,
    6 => :not_leader_for_partition,
    7 => :request_timed_out,
    8 => :broker_not_available,
    9 => :replica_not_available,
    10 => :message_size_too_large,
    11 => :stale_controller_epoch,
    12 => :offset_metadata_too_large,
    14 => :offset_loads_in_progress,
    15 => :consumer_coordinator_not_available,
    16 => :not_coordinator_for_consumer,
    17 => :invalid_topic,
    18 => :record_list_too_large,
    19 => :not_enough_replicas,
    20 => :not_enough_replicas_after_append,
    21 => :invalid_required_acks,
    22 => :illegal_generation,
    23 => :inconsistent_group_protocol,
    24 => :invalid_group_id,
    25 => :unknown_member_id,
    26 => :invalid_session_timeout,
    27 => :rebalance_in_progress,
    28 => :invalid_commit_offset,
    29 => :topic_authorization_failed,
    30 => :group_authorization_failed,
    31 => :cluster_authorization_failed,
    32 => :invalid_timestamp,
    33 => :unsupported_sasl_mechanism,
    34 => :illegal_sasl_state,
    35 => :unsupported_version,
    36 => :topic_already_exists,
    37 => :invalid_partitions,
    38 => :invalid_replication_factor,
    39 => :invalid_replica_assignment,
    40 => :invalid_config,
    41 => :not_controller,
    42 => :invalid_request,
    43 => :unsupported_for_message_format
  }

  @spec error(integer) :: atom | integer
  def error(err_no) do
    case err_no do
      -1 -> :unknown_error
      _ -> @error_map[err_no] || err_no
    end
  end
end
