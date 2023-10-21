defmodule KafkaEx do
  @moduledoc """
  Kafka API

  This module is the main API for users of the KafkaEx library.

  Most of these functions either use the default worker (registered as
  `:kafka_ex`) by default or can take a registered name or pid via a
  `worker_name` option.

  ```
  # create an unnamed worker
  {:ok, pid} = KafkaEx.create_worker(:no_name)

  KafkaEx.fetch("some_topic", 0, worker_name: pid)
  ```
  """

  use Application
  alias KafkaEx.Config
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

  @doc """
  create_worker creates KafkaEx workers

  Optional arguments(KeywordList)
  - consumer_group: Name of the group of consumers, `:no_consumer_group` should be passed for Kafka < 0.8.2, defaults to `Application.get_env(:kafka_ex, :consumer_group)`
  - uris: List of brokers in `{"host", port}` or comma separated value `"host:port,host:port"` form, defaults to `Application.get_env(:kafka_ex, :brokers)`
  - metadata_update_interval: How often `kafka_ex` would update the Kafka cluster metadata information in milliseconds, default is 30000
  - consumer_group_update_interval: How often `kafka_ex` would update the Kafka cluster consumer_groups information in milliseconds, default is 30000
  - use_ssl: Boolean flag specifying if ssl should be used for the connection by the worker to Kafka, default is false
  - ssl_options: see SSL OPTION DESCRIPTIONS - CLIENT SIDE at http://erlang.org/doc/man/ssl.html, default is []

  Returns `{:error, error_description}` on invalid arguments

  ## Example

  ```elixir
  iex> KafkaEx.create_worker(:pr) # where :pr is the name of the worker created
  {:ok, #PID<0.171.0>}
  iex> KafkaEx.create_worker(:pr, uris: [{"localhost", 9092}])
  {:ok, #PID<0.172.0>}
  iex> KafkaEx.create_worker(:pr, [uris: [{"localhost", 9092}], consumer_group: "foo"])
  {:ok, #PID<0.173.0>}
  iex> KafkaEx.create_worker(:pr, consumer_group: nil)
  {:error, :invalid_consumer_group}
  ```
  """
  @spec create_worker(atom, KafkaEx.worker_init()) ::
          Supervisor.on_start_child()
  def create_worker(name, worker_init \\ []) do
    server_impl = Config.server_impl()

    case build_worker_options(worker_init) do
      {:ok, worker_init} ->
        KafkaEx.Supervisor.start_child(server_impl, [worker_init, name])

      {:error, error} ->
        {:error, error}
    end
  end

  @doc """
  Stop a worker created with create_worker/2

  Returns `:ok` on success or `:error` if `worker` is not a valid worker
  """
  @spec stop_worker(atom | pid) ::
          :ok
          | {:error, :not_found}
          | {:error, :simple_one_for_one}
  def stop_worker(worker) do
    KafkaEx.Supervisor.stop_child(worker)
  end

  @doc """
  Returns the name of the consumer group for the given worker.

  Worker may be an atom or pid.  The default worker is used by default.
  """
  @spec consumer_group(atom | pid) :: binary | :no_consumer_group
  def consumer_group(worker \\ Config.default_worker()) do
    Server.call(worker, :consumer_group)
  end

  @doc """
  Sends a request to describe a group identified by its name.
  We support only one consumer group per request for now, as we don't
  group requests by group coordinator.
  This is a new client implementation, and is not compatible with the old clients
  """
  @spec describe_group(binary, Keyword.t()) :: {:ok, any} | {:error, any}
  def describe_group(consumer_group_name, opts \\ []) do
    worker_name = Keyword.get(opts, :worker_name, Config.default_worker())

    case Server.call(worker_name, {:describe_groups, [consumer_group_name]}) do
      {:ok, [group]} -> {:ok, group}
      {:error, error} -> {:error, error}
    end
  end

  @doc """
  Sends a request to join a consumer group.
  """
  @spec join_group(JoinGroupRequest.t(), Keyword.t()) :: JoinGroupResponse.t()
  def join_group(request, opts \\ []) do
    worker_name = Keyword.get(opts, :worker_name, Config.default_worker())
    timeout = Keyword.get(opts, :timeout)
    Server.call(worker_name, {:join_group, request, timeout}, opts)
  end

  @doc """
  Sends a request to synchronize with a consumer group.
  """
  @spec sync_group(SyncGroupRequest.t(), Keyword.t()) :: SyncGroupResponse.t()
  def sync_group(request, opts \\ []) do
    worker_name = Keyword.get(opts, :worker_name, Config.default_worker())
    timeout = Keyword.get(opts, :timeout)
    Server.call(worker_name, {:sync_group, request, timeout}, opts)
  end

  @doc """
  Sends a request to leave a consumer group.
  """
  @spec leave_group(LeaveGroupRequest.t(), Keyword.t()) ::
          LeaveGroupResponse.t()
  def leave_group(request, opts \\ []) do
    worker_name = Keyword.get(opts, :worker_name, Config.default_worker())
    timeout = Keyword.get(opts, :timeout)
    Server.call(worker_name, {:leave_group, request, timeout}, opts)
  end

  @doc """
  Sends a heartbeat to maintain membership in a consumer group.
  """
  @spec heartbeat(HeartbeatRequest.t(), Keyword.t()) :: HeartbeatResponse.t()
  def heartbeat(request, opts \\ []) do
    worker_name = Keyword.get(opts, :worker_name, Config.default_worker())
    timeout = Keyword.get(opts, :timeout)
    Server.call(worker_name, {:heartbeat, request, timeout}, opts)
  end

  @doc """
  Return metadata for the given topic; returns for all topics if topic is empty string

  Optional arguments(KeywordList)
  - worker_name: the worker we want to run this metadata request through, when none is provided the default worker `:kafka_ex` is used
  - topic: name of the topic for which metadata is requested, when none is provided all metadata is retrieved

  ## Example

  ```elixir
  iex> KafkaEx.create_worker(:mt)
  iex> KafkaEx.metadata(topic: "foo", worker_name: :mt)
  %KafkaEx.Protocol.Metadata.Response{brokers: [%KafkaEx.Protocol.Metadata.Broker{host: "192.168.59.103",
     node_id: 49162, port: 49162, socket: nil}],
   topic_metadatas: [%KafkaEx.Protocol.Metadata.TopicMetadata{error_code: 0,
     partition_metadatas: [%KafkaEx.Protocol.Metadata.PartitionMetadata{error_code: 0,
       isrs: [49162], leader: 49162, partition_id: 0, replicas: [49162]}],
     topic: "foo"}]}
  ```
  """
  @spec metadata(Keyword.t()) :: MetadataResponse.t()
  def metadata(opts \\ []) do
    worker_name = Keyword.get(opts, :worker_name, Config.default_worker())
    topic = Keyword.get(opts, :topic, "")
    Server.call(worker_name, {:metadata, topic}, opts)
  end

  @spec consumer_group_metadata(atom, binary) :: ConsumerMetadataResponse.t()
  def consumer_group_metadata(worker_name, supplied_consumer_group) do
    Server.call(
      worker_name,
      {:consumer_group_metadata, supplied_consumer_group}
    )
  end

  @doc """
  Get the offset of the latest message written to Kafka

  ## Example

  ```elixir
  iex> KafkaEx.latest_offset("foo", 0)
  [%KafkaEx.Protocol.Offset.Response{partition_offsets: [%{error_code: 0, offsets: [16], partition: 0}], topic: "foo"}]
  ```
  """
  @spec latest_offset(binary, integer, atom | pid) ::
          [OffsetResponse.t()] | :topic_not_found
  def latest_offset(topic, partition, name \\ Config.default_worker()),
    do: offset(topic, partition, :latest, name)

  @doc """
  Get the offset of the earliest message still persistent in Kafka

  ## Example

  ```elixir
  iex> KafkaEx.earliest_offset("foo", 0)
  [%KafkaEx.Protocol.Offset.Response{partition_offsets: [%{error_code: 0, offset: [0], partition: 0}], topic: "foo"}]
  ```
  """
  @spec earliest_offset(binary, integer, atom | pid) ::
          [OffsetResponse.t()] | :topic_not_found
  def earliest_offset(topic, partition, name \\ Config.default_worker()),
    do: offset(topic, partition, :earliest, name)

  @doc """
  Get the offset of the message sent at the specified date/time

  ## Example

  ```elixir
  iex> KafkaEx.offset("foo", 0, {{2015, 3, 29}, {23, 56, 40}}) # Note that the time specified should match/be ahead of time on the server that kafka runs
  [%KafkaEx.Protocol.Offset.Response{partition_offsets: [%{error_code: 0, offset: [256], partition: 0}], topic: "foo"}]
  ```
  """
  @spec offset(
          binary,
          number,
          :calendar.datetime() | :earliest | :latest,
          atom | pid
        ) :: [OffsetResponse.t()] | :topic_not_found
  def offset(topic, partition, time, name \\ Config.default_worker()) do
    Server.call(name, {:offset, topic, partition, time})
  end

  @wait_time 10
  @min_bytes 1
  @max_bytes 1_000_000

  @doc """
  Fetch a set of messages from Kafka from the given topic and partition ID

  Optional arguments(KeywordList)
  - offset: When supplied the fetch would start from this offset, otherwise would start from the last committed offset of the consumer_group the worker belongs to. For Kafka < 0.8.2 you should explicitly specify this.
  - worker_name: the worker we want to run this fetch request through. Default is :kafka_ex
  - wait_time: maximum amount of time in milliseconds to block waiting if insufficient data is available at the time the request is issued. Default is 10
  - min_bytes: minimum number of bytes of messages that must be available to give a response. If the client sets this to 0 the server will always respond immediately, however if there is no new data since their last request they will just get back empty message sets. If this is set to 1, the server will respond as soon as at least one partition has at least 1 byte of data or the specified timeout occurs. By setting higher values in combination with the timeout the consumer can tune for throughput and trade a little additional latency for reading only large chunks of data (e.g. setting wait_time to 100 and setting min_bytes 64000 would allow the server to wait up to 100ms to try to accumulate 64k of data before responding). Default is 1
  - max_bytes: maximum bytes to include in the message set for this partition. This helps bound the size of the response. Default is 1,000,000
  - auto_commit: specifies if the last offset should be committed or not. Default is true. You must set this to false when using Kafka < 0.8.2 or `:no_consumer_group`.
  - api_version: Version of the Fetch API message to send (Kayrock client only, default: 0)
  - offset_commit_api_version: Version of the OffsetCommit API message to send
    (Kayrock client only, only relevant for auto commit, default: 0, use 2+ to
    store offsets in Kafka instead of Zookeeper)

  ## Example

  ```elixir
  iex> KafkaEx.fetch("foo", 0, offset: 0)
  [
    %KafkaEx.Protocol.Fetch.Response{partitions: [
      %{error_code: 0, hw_mark_offset: 1, message_set: [
        %{attributes: 0, crc: 748947812, key: nil, offset: 0, value: "hey foo"}
      ], partition: 0}
    ], topic: "foo"}
  ]
  ```
  """
  @spec fetch(binary, number, Keyword.t()) ::
          [FetchResponse.t()] | :topic_not_found
  def fetch(topic, partition, opts \\ []) do
    worker_name = Keyword.get(opts, :worker_name, Config.default_worker())
    supplied_offset = Keyword.get(opts, :offset)
    wait_time = Keyword.get(opts, :wait_time, @wait_time)
    min_bytes = Keyword.get(opts, :min_bytes, @min_bytes)
    max_bytes = Keyword.get(opts, :max_bytes, @max_bytes)
    auto_commit = Keyword.get(opts, :auto_commit, true)

    # NOTE api_version is used by the new client to allow
    # compatibility with newer message formats and is ignored by the legacy
    # server implementations.
    api_version = Keyword.get(opts, :api_version, 0)
    # same for offset_commit_api_version
    offset_commit_api_version = Keyword.get(opts, :offset_commit_api_version, 0)

    # By default, it makes sense to synchronize the API version of the offset_commit and the offset_fetch
    # operations, otherwise we might commit the offsets in zookeeper and read them from Kafka, meaning
    # that the value would be incorrect.
    offset_fetch_api_version =
      Keyword.get(opts, :offset_fetch_api_version, offset_commit_api_version)

    retrieved_offset =
      current_offset(
        supplied_offset,
        partition,
        topic,
        worker_name,
        offset_fetch_api_version,
        nil
      )

    Server.call(
      worker_name,
      {:fetch,
       %FetchRequest{
         auto_commit: auto_commit,
         topic: topic,
         partition: partition,
         offset: retrieved_offset,
         wait_time: wait_time,
         min_bytes: min_bytes,
         max_bytes: max_bytes,
         api_version: api_version,
         offset_commit_api_version: offset_commit_api_version
       }},
      opts
    )
  end

  @spec offset_commit(atom, OffsetCommitRequest.t()) :: [
          OffsetCommitResponse.t()
        ]
  def offset_commit(worker_name, offset_commit_request) do
    Server.call(worker_name, {:offset_commit, offset_commit_request})
  end

  @spec offset_fetch(atom, OffsetFetchRequest.t()) ::
          [OffsetFetchResponse.t()] | :topic_not_found
  def offset_fetch(worker_name, offset_fetch_request) do
    Server.call(worker_name, {:offset_fetch, offset_fetch_request})
  end

  @doc """
  Produces batch messages to kafka logs

  Optional arguments(KeywordList)
  - worker_name: the worker we want to run this metadata request through, when none is provided the default worker `:kafka_ex` is used

  ## Example

  ```elixir
  iex> KafkaEx.produce(%KafkaEx.Protocol.Produce.Request{topic: "foo", partition: 0, required_acks: 1, messages: [%KafkaEx.Protocol.Produce.Message{value: "hey"}]})
  {:ok, 9772}
  iex> KafkaEx.produce(%KafkaEx.Protocol.Produce.Request{topic: "foo", partition: 0, required_acks: 1, messages: [%KafkaEx.Protocol.Produce.Message{value: "hey"}]}, worker_name: :pr)
  {:ok, 9773}
  ```
  """
  @spec produce(ProduceRequest.t(), Keyword.t()) ::
          nil
          | :ok
          | {:ok, integer}
          | {:error, :closed}
          | {:error, :inet.posix()}
          | {:error, any}
          | iodata
          | :leader_not_available
  def produce(produce_request, opts \\ []) do
    worker_name = Keyword.get(opts, :worker_name, Config.default_worker())
    Server.call(worker_name, {:produce, produce_request}, opts)
  end

  @doc """
  Produces messages to Kafka logs (this is deprecated, use KafkaEx.produce/2 instead)
  Optional arguments(KeywordList)
  - worker_name: the worker we want to run this metadata request through, when none is provided the default worker `:kafka_ex` is used
  - key: is used for partition assignment, can be nil, when none is provided it is defaulted to nil
  - required_acks: indicates how many acknowledgements the servers should receive before responding to the request. If it is 0 the server will not send any response (this is the only case where the server will not reply to a request). If it is 1, the server will wait the data is written to the local log before sending a response. If it is -1 the server will block until the message is committed by all in sync replicas before sending a response. For any number > 1 the server will block waiting for this number of acknowledgements to occur (but the server will never wait for more acknowledgements than there are in-sync replicas), default is 0
  - timeout: provides a maximum time in milliseconds the server can await the receipt of the number of acknowledgements in RequiredAcks, default is 100 milliseconds
  - compression: specifies the compression type (:none, :snappy, :gzip)
  - api_version: Version of the Fetch API message to send (Kayrock client only, default: 0)
  - timestamp: unix epoch timestamp in milliseconds for the message
      (Kayrock client only, default: nil, must be using api_version >= 3)

  ## Example

  ```elixir
  iex> KafkaEx.produce("bar", 0, "hey")
  :ok
  iex> KafkaEx.produce("foo", 0, "hey", [worker_name: :pr, required_acks: 1])
  {:ok, 9771}
  ```
  """
  @spec produce(binary, number, binary, Keyword.t()) ::
          nil
          | :ok
          | {:ok, integer}
          | {:error, :closed}
          | {:error, :inet.posix()}
          | {:error, any}
          | iodata
          | :leader_not_available
  def produce(topic, partition, value, opts \\ []) do
    key = Keyword.get(opts, :key, "")
    required_acks = Keyword.get(opts, :required_acks, 0)
    timeout = Keyword.get(opts, :timeout, 100)
    compression = Keyword.get(opts, :compression, :none)
    timestamp = Keyword.get(opts, :timestamp)

    produce_request = %ProduceRequest{
      topic: topic,
      partition: partition,
      required_acks: required_acks,
      timeout: timeout,
      compression: compression,
      messages: [%Message{key: key, value: value, timestamp: timestamp}],
      api_version: Keyword.get(opts, :api_version, 0)
    }

    produce(produce_request, opts)
  end

  @doc ~S"""
  Returns a streamable struct that may be used for consuming messages.

  The returned struct is compatible with the `Stream` and `Enum` modules.  Some
  important usage notes follow; see below for a detailed list of options.

  ```elixir
  iex> KafkaEx.produce("foo", 0, "hey")
  :ok
  iex> KafkaEx.produce("foo", 0, "hi")
  :ok
  iex> stream = KafkaEx.stream("foo", 0)
  %KafkaEx.Stream{...}
  iex> Enum.take(stream, 2)
  [%KafkaEx.Protocol.Fetch.Message{attributes: 0, crc: 1784030606, key: "",
      offset: 0, value: "hey"},
   %KafkaEx.Protocol.Fetch.Message{attributes: 0, crc: 3776653906, key: "",
       offset: 1, value: "hi"}]
  iex> stream |> Stream.map(fn(msg) -> IO.puts(msg.value) end) |> Stream.run
  "hey"
  "hi"
  #  NOTE this will block!  See below.
  ```

  ## Reusing streams

  Reusing the same `KafkaEx.Stream` struct results in consuming the same
  messages multiple times.  This is by design and mirrors the functionality of
  `File.stream!/3`.  If you want to reuse the same stream struct, update its
  `:offset` before reuse.

  ```
  iex> stream = KafkaEx.stream("foo", 0)
  iex> [m1, m2] = Enum.take(stream, 2)
  iex> [m1, m2] = Enum.take(stream, 2)   # these will be the same messages
  iex> stream = %{stream | fetch_request: %{stream.fetch_request | offset: m2.offset + 1}}
  iex> [m3, m4] = Enum.take(stream, 2)   # new messages
  ```

  ## Streams block at log end

  By default, the stream consumes indefinitely and will block at log end until
  new messages are available.  Use the `no_wait_at_logend: true` option to have
  the stream halt when no more messages are available.  This mirrors the
  command line arguments of
  [SimpleConsumerShell](https://cwiki.apache.org/confluence/display/KAFKA/System+Tools#SystemTools-SimpleConsumerShell).

  Note that this means that fetches will return up to as many messages
  as are immediately available in the partition, regardless of arguments.

  ```
  iex> Enum.map(1..3, fn(ix) -> KafkaEx.produce("bar", 0, "Msg #{ix}") end)
  iex> stream = KafkaEx.stream("bar", 0, no_wait_at_logend: true, offset: 0)
  iex> Enum.map(stream, fn(m) -> m.value end) # does not block
  ["Msg 1", "Msg 2", "Msg 3"]
  iex> stream |> Stream.map(fn(m) -> m.value end) |> Enum.take(10)
  # only 3 messages are available
  ["Msg 1", "Msg 2", "Msg 3"]
  ```

  ## Consumer group and auto commit

  If you pass a value for the `consumer_group` option and true for
  `auto_commit`, the offset of the last message consumed will be committed to
  the broker during each cycle.

  For example, suppose we start at the beginning of a partition with millions
  of messages and the `max_bytes` setting is such that each `fetch` request
  gets 25 messages.  In this setting, we will (roughly) be committing offsets
  25, 50, 75, etc.

  Note that offsets are committed immediately after messages are retrieved
  and before you know if you have successfully consumed them.  It is
  therefore possible that you could miss messages if your consumer crashes in
  the middle of consuming a batch, effectively losing the guarantee of
  at-least-once delivery.  If you need this guarantee, we recommend that you
  construct a GenServer-based consumer module and manage your commits manually.

  ```
  iex> Enum.map(1..10, fn(ix) -> KafkaEx.produce("baz", 0, "Msg #{ix}") end)
  iex> stream = KafkaEx.stream("baz", 0, consumer_group: "my_consumer", auto_commit: true)
  iex> stream |> Enum.take(2) |> Enum.map(fn(msg) -> msg.value end)
  ["Msg 1", "Msg 2"]
  iex> stream |> Enum.take(2) |> Enum.map(fn(msg) -> msg.value end)
  ["Msg 1", "Msg 2"]  # same values
  iex> stream2 = KafkaEx.stream("baz", 0, consumer_group: "my_consumer", auto_commit: true)
  iex> stream2 |> Enum.take(1) |> Enum.map(fn(msg) -> msg.value end)
  ["Msg 3"] # stream2 got the next available offset
  ```

  ## Options

  `KafkaEx.stream/3` accepts a keyword list of options for the third argument.

  - `no_wait_at_logend` (boolean): Set this to true to halt the stream when
  there are no more messages available.  Defaults to false, i.e., the stream
  blocks to wait for new messages.

  - `worker_name` (term): The KafkaEx worker to use for communication with the
  brokers.  Defaults to `:kafka_ex` (the default worker).

  - `consumer_group` (string): Name of the consumer group used for the initial
  offset fetch and automatic offset commit (if `auto_commit` is true). Omit
  this value or use `:no_consumer_group` to not use a consumer group (default).
  Consumer groups are not compatible with Kafka < 0.8.2.

  - `offset` (integer): The offset from which to start fetching.  By default,
  this is the last available offset of the partition when no consumer group is
  specified.  When a consumer group is specified, the next message after the
  last committed offset is used.  For Kafka < 0.8.2 you must explicitly specify
  an offset.

  - `auto_commit` (boolean): If true, the stream automatically commits offsets
  of fetched messages.  See discussion above.

  - `api_versions` (map): Allows overriding api versions for `:fetch`,
  `:offset_fetch`, and `:offset_commit` when using the Kayrock client.  Defaults to
  `%{fetch: 0, offset_fetch: 0, offset_commit: 0}`.  Use
  `%{fetch: 3, offset_fetch: 3, offset_commit: 3}` with the kayrock client to
  achieve offsets stored in Kafka (instead of zookeeper) and messages fetched
  with timestamps.
  """
  @spec stream(binary, integer, Keyword.t()) :: KafkaEx.Stream.t()
  def stream(topic, partition, opts \\ []) do
    auto_commit = Keyword.get(opts, :auto_commit, true)
    consumer_group = Keyword.get(opts, :consumer_group)
    max_bytes = Keyword.get(opts, :max_bytes, @max_bytes)
    min_bytes = Keyword.get(opts, :min_bytes, @min_bytes)
    supplied_offset = Keyword.get(opts, :offset)
    worker_name = Keyword.get(opts, :worker_name, Config.default_worker())
    no_wait_at_logend = Keyword.get(opts, :no_wait_at_logend, false)
    wait_time = Keyword.get(opts, :wait_time, @wait_time)

    default_api_versions = %{fetch: 0, offset_fetch: 0, offset_commit: 0}
    api_versions = Keyword.get(opts, :api_versions, %{})
    api_versions = Map.merge(default_api_versions, api_versions)
    offset_fetch_api_version = Map.fetch!(api_versions, :offset_fetch)

    retrieved_offset =
      current_offset(
        supplied_offset,
        partition,
        topic,
        worker_name,
        offset_fetch_api_version,
        consumer_group
      )

    fetch_request = %FetchRequest{
      auto_commit: auto_commit,
      topic: topic,
      partition: partition,
      offset: retrieved_offset,
      wait_time: wait_time,
      min_bytes: min_bytes,
      max_bytes: max_bytes,
      api_version: Map.fetch!(api_versions, :fetch),
      offset_commit_api_version: Map.fetch!(api_versions, :offset_commit)
    }

    %Stream{
      worker_name: worker_name,
      fetch_request: fetch_request,
      consumer_group: consumer_group,
      no_wait_at_logend: no_wait_at_logend,
      api_versions: api_versions
    }
  end

  @doc """
  Start and link a worker outside of a supervision tree

  This takes the same arguments as `create_worker/2` except that it adds

  - `server_impl` - This is the GenServer that will be used for the
    client genserver implementation - e.g., `KafkaEx.Server0P8P0`,
    `KafkaEx.Server0P10AndLater`, `KafkaEx.New.Client`.  Defaults to the value
    determined by the `kafka_version` setting.
  """
  @spec start_link_worker(atom, [
          KafkaEx.worker_setting() | {:server_impl, module}
        ]) :: GenServer.on_start()
  def start_link_worker(name, worker_init \\ []) do
    {server_impl, worker_init} = Keyword.pop(worker_init, :server_impl, Config.server_impl())

    {:ok, full_worker_init} = build_worker_options(worker_init)
    server_impl.start_link(full_worker_init, name)
  end

  @doc """
  Builds options to be used with workers

  Merges the given options with defaults from the application env config.
  Returns `{:error, :invalid_consumer_options}` if the consumer group
  configuration is invalid, and `{:ok, merged_options}` otherwise.

  Note this happens automatically when using `KafkaEx.create_worker`.
  """
  @spec build_worker_options(worker_init) ::
          {:ok, worker_init} | {:error, :invalid_consumer_group}
  def build_worker_options(worker_init) do
    defaults = [
      uris: Config.brokers(),
      consumer_group: Config.consumer_group(),
      use_ssl: Config.use_ssl(),
      ssl_options: Config.ssl_options()
    ]

    worker_init = Keyword.merge(defaults, worker_init)

    supplied_consumer_group = Keyword.get(worker_init, :consumer_group)

    if valid_consumer_group?(supplied_consumer_group) do
      {:ok, worker_init}
    else
      {:error, :invalid_consumer_group}
    end
  end

  defp current_offset(
         supplied_offset,
         partition,
         topic,
         worker_name,
         api_version,
         consumer_group_name \\ nil
       )

  defp current_offset(
         supplied_offset,
         _partition,
         _topic,
         _worker_name,
         _api_version,
         _consumer_group
       )
       when not is_nil(supplied_offset),
       do: supplied_offset

  defp current_offset(
         nil,
         partition,
         topic,
         worker_name,
         api_version,
         consumer_group_name
       ) do
    last_offset =
      worker_name
      |> offset_fetch(%OffsetFetchRequest{
        topic: topic,
        partition: partition,
        api_version: api_version,
        consumer_group: consumer_group_name
      })
      |> OffsetFetchResponse.last_offset()

    if last_offset < 0 do
      topic
      |> earliest_offset(partition, worker_name)
      |> OffsetResponse.extract_offset()
    else
      last_offset + 1
    end
  end

  @doc """
  Returns true if the input is a valid consumer group or :no_consumer_group
  """
  @spec valid_consumer_group?(any) :: boolean
  def valid_consumer_group?(:no_consumer_group), do: true
  def valid_consumer_group?(b) when is_binary(b), do: byte_size(b) > 0
  def valid_consumer_group?(_), do: false

  @doc """
  Retrieve supported api versions for each api key.
  """
  @spec api_versions(Keyword.t()) :: ApiVersionsResponse.t()
  def api_versions(opts \\ []) do
    worker_name = Keyword.get(opts, :worker_name, Config.default_worker())
    Server.call(worker_name, {:api_versions})
  end

  @doc """
  Create topics. Must provide a list of CreateTopicsRequest, each containing
  all the information needed for the creation of a new topic.

  See available topic configuration options at https://docs.confluent.io/platform/current/installation/configuration/topic-configs.html

  ## Example

  ```elixir
  iex> KafkaEx.start_link_worker(:my_kafka_worker)
  {:ok, #PID<0.659.0>}
  iex> KafkaEx.create_topics(
  ...>  [
  ...>   %KafkaEx.Protocol.CreateTopics.TopicRequest{
  ...>     topic: "my_topic_name",
  ...>      num_partitions: 1,
  ...>      replication_factor: 1,
  ...>      replica_assignment: [],
  ...>      config_entries: [
  ...>        %{config_name: "cleanup.policy", config_value: "delete"},
  ...>        %{config_name: "delete.retention.ms", config_value: "864000000"} # 1 day
  ...>      ]
  ...>    }
  ...>  ],
  ...>  timeout: 10_000,
  ...>  worker_name: :my_kafka_worker
  ...> )
  %KafkaEx.Protocol.CreateTopics.Response{
    topic_errors: [
      %KafkaEx.Protocol.CreateTopics.TopicError{
        error_code: :no_error,
        topic_name: "my_topic_name"
      }
    ]
  }
  ```
  """
  @spec create_topics([CreateTopicsRequest.t()], Keyword.t()) ::
          CreateTopicsResponse.t()
  def create_topics(requests, opts \\ []) do
    worker_name = Keyword.get(opts, :worker_name, Config.default_worker())
    timeout = Keyword.get(opts, :timeout)
    Server.call(worker_name, {:create_topics, requests, timeout})
  end

  @doc """
  Delete topics. Must provide a list of topic names.

  ## Example

  ```elixir
  iex> KafkaEx.delete_topics(["my_topic_name"], [])
  %KafkaEx.Protocol.DeleteTopics.Response{
    topic_errors: [
      %KafkaEx.Protocol.DeleteTopics.TopicError{
        error_code: :no_error,
        topic_name: "my_topic_name"
      }
    ]
  }
  ```
  """
  @spec delete_topics([String.t()], Keyword.t()) :: DeleteTopicsResponse.t()
  def delete_topics(topics, opts \\ []) do
    worker_name = Keyword.get(opts, :worker_name, Config.default_worker())
    timeout = Keyword.get(opts, :timeout)
    Server.call(worker_name, {:delete_topics, topics, timeout})
  end

  # OTP API
  def start(_type, _args) do
    max_restarts = Application.get_env(:kafka_ex, :max_restarts, 10)
    max_seconds = Application.get_env(:kafka_ex, :max_seconds, 60)

    {:ok, pid} =
      KafkaEx.Supervisor.start_link(
        max_restarts,
        max_seconds
      )

    if Config.disable_default_worker() do
      {:ok, pid}
    else
      case KafkaEx.create_worker(Config.default_worker(), []) do
        {:error, reason} -> {:error, reason}
        {:ok, _} -> {:ok, pid}
      end
    end
  end
end
