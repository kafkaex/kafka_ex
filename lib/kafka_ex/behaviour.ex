defmodule KafkaEx.Behaviour do
  @moduledoc """
  Behaviour for main Kafka Ex API
  """
  alias KafkaEx.Protocol.ConsumerMetadata.Response, as: ConsumerMetadataResponse
  alias KafkaEx.Protocol.Fetch.Response, as: FetchResponse
  alias KafkaEx.Protocol.Fetch.Request, as: FetchRequest
  alias KafkaEx.Protocol.Heartbeat.Request, as: HeartbeatRequest
  alias KafkaEx.Protocol.Heartbeat.Response, as: HeartbeatResponse
  alias KafkaEx.Protocol.JoinGroup.Request, as: JoinGroupRequest
  alias KafkaEx.Protocol.JoinGroup.Response, as: JoinGroupResponse
  alias KafkaEx.Protocol.LeaveGroup.Request, as: LeaveGroupRequest
  alias KafkaEx.Protocol.LeaveGroup.Response, as: LeaveGroupResponse
  alias KafkaEx.Protocol.Metadata.Response, as: MetadataResponse
  alias KafkaEx.Protocol.Offset.Response, as: OffsetResponse
  alias KafkaEx.Protocol.OffsetCommit.Request, as: OffsetCommitRequest
  alias KafkaEx.Protocol.OffsetCommit.Response, as: OffsetCommitResponse
  alias KafkaEx.Protocol.OffsetFetch.Response, as: OffsetFetchResponse
  alias KafkaEx.Protocol.OffsetFetch.Request, as: OffsetFetchRequest
  alias KafkaEx.Protocol.Produce.Request, as: ProduceRequest
  alias KafkaEx.Protocol.Produce.Message
  alias KafkaEx.Protocol.SyncGroup.Request, as: SyncGroupRequest
  alias KafkaEx.Protocol.SyncGroup.Response, as: SyncGroupResponse
  alias KafkaEx.Protocol.CreateTopics.TopicRequest, as: CreateTopicsRequest
  alias KafkaEx.Protocol.CreateTopics.Response, as: CreateTopicsResponse
  alias KafkaEx.Protocol.DeleteTopics.Response, as: DeleteTopicsResponse
  alias KafkaEx.Protocol.ApiVersions.Response, as: ApiVersionsResponse
  alias KafkaEx.Server
  alias KafkaEx.Stream

  @type worker_name :: atom
  @type consumer_group_name :: binary
  @type topic :: binary
  @type partition :: non_neg_integer
  @type message_value :: binary

  @type uri() :: [{binary | [char], number}]
  @type worker_init :: [worker_setting]
  @type ssl_options :: [
          {:cacertfile, binary}
          | {:certfile, binary}
          | {:keyfile, binary}
          | {:password, binary}
        ]
  @type worker_setting ::
          {:uris, uri}
          | {:consumer_group, binary | :no_consumer_group}
          | {:metadata_update_interval, non_neg_integer}
          | {:consumer_group_update_interval, non_neg_integer}
          | {:ssl_options, ssl_options}
          | {:initial_topics, [binary]}

  @callback create_worker(worker_name, KafkaEx.worker_init()) ::
              Supervisor.on_start_child()

  @callback stop_worker(worker_name | pid) ::
              :ok | {:error, :not_found | :simple_one_for_one}

  @callback consumer_group(worker_name | pid) ::
              consumer_group_name | :no_consumer_group

  @callback join_group(JoinGroupRequest.t(), Keyword.t()) ::
              JoinGroupResponse.t()

  @callback sync_group(SyncGroupRequest.t(), Keyword.t()) ::
              SyncGroupResponse.t()

  @callback leave_group(LeaveGroupRequest.t(), Keyword.t()) ::
              LeaveGroupResponse.t()

  @callback heartbeat(HeartbeatRequest.t(), Keyword.t()) ::
              HeartbeatResponse.t()

  @callback metadata(Keyword.t()) :: MetadataResponse.t()

  @callback consumer_group_metadata(worker_name, consumer_group_name) ::
              ConsumerMetadataResponse.t()

  @callback latest_offset(topic, partition, worker_name | pid) ::
              [OffsetResponse.t()] | :topic_not_found

  @callback earliest_offset(topic, partition, worker_name | pid) ::
              [OffsetResponse.t()] | :topic_not_found

  @callback offset(
              topic,
              partition,
              :calendar.datetime() | :earliest | :latest,
              worker_name | pid
            ) :: [OffsetResponse.t()] | :topic_not_found

  @callback fetch(topic, partition, Keyword.t()) ::
              [FetchResponse.t()] | :topic_not_found

  @callback offset_commit(worker_name, OffsetCommitRequest.t()) :: [
              OffsetCommitResponse.t()
            ]

  @callback offset_fetch(worker_name, OffsetFetchRequest.t()) ::
              [OffsetFetchResponse.t()] | :topic_not_found

  @callback produce(ProduceRequest.t(), Keyword.t()) ::
              nil
              | :ok
              | {:ok, integer}
              | {:error, :closed}
              | {:error, :inet.posix()}
              | {:error, any}
              | iodata
              | :leader_not_available

  @callback produce(topic, partition, message_value, Keyword.t()) ::
              nil
              | :ok
              | {:ok, integer}
              | {:error, :closed}
              | {:error, :inet.posix()}
              | {:error, any}
              | iodata
              | :leader_not_available

  @callback stream(topic, partition, Keyword.t()) :: KafkaEx.Stream.t()

  @callback start_link_worker(worker_name, [
              KafkaEx.worker_setting() | {:server_impl, module}
            ]) :: GenServer.on_start()

  @callback build_worker_options(worker_init) ::
              {:ok, worker_init} | {:error, :invalid_consumer_group}

  @callback valid_consumer_group?(any) :: boolean

  @callback api_versions(Keyword.t()) :: ApiVersionsResponse.t()

  @callback create_topics([CreateTopicsRequest.t()], Keyword.t()) ::
              CreateTopicsResponse.t()

  @callback delete_topics([String.t()], Keyword.t()) :: DeleteTopicsResponse.t()
end
