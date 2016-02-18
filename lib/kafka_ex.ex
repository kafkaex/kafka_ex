defmodule KafkaEx do
  use Application
  @type uri() :: [{binary|char_list, number}]
  @type worker_init :: [{:uris, uri}, {:consumer_group, binary|false}, {:sync_timeout, non_neg_integer}]

  @doc """
  create_worker creates KafkaEx workers

  Optional arguments(KeywordList)
  - consumer_group: Name of the group of consumers, `false` should be passed for Kafka < 0.8.2, default is "kafka_ex"
  - metadata_update_interval: How often `kafka_ex` would update the Kafka cluster metadata information in milliseconds, default is 30000
  - consumer_group_update_interval: How often `kafka_ex` would update the Kafka cluster consumer_groups information in milliseconds, default is 30000
  - sync_timeout: Timeout for synchronous requests to kafka in milliseconds, default is 1000

  ## Example

  ```elixir
  iex> KafkaEx.create_worker(:pr) # where :pr is the name of the worker created
  {:ok, #PID<0.171.0>}
  iex> KafkaEx.create_worker(:pr, uris: [{"localhost", 9092}])
  {:ok, #PID<0.172.0>}
  iex> KafkaEx.create_worker(:pr, [uris: [{"localhost", 9092}], consumer_group: "foo"])
  {:ok, #PID<0.173.0>}
  iex> KafkaEx.create_worker(:pr, [uris: [{"localhost", 9092}], consumer_group: "foo", sync_timeout: 2000])
  {:ok, #PID<0.173.0>}
  ```
  """
  @spec create_worker(atom, KafkaEx.worker_init) :: Supervisor.on_start_child
  def create_worker(name, worker_init \\ [])

  def create_worker(name, worker_init) do
    worker_init = case worker_init do
      [] -> [uris: Application.get_env(:kafka_ex, :brokers)]
      _   -> worker_init
    end

    Supervisor.start_child(KafkaEx.Supervisor, [worker_init, name])
  end

  @doc """
  Return metadata for the given topic; returns for all topics if topic is empty string

  Optional arguments(KeywordList)
  - worker_name: the worker we want to run this metadata request through, when none is provided the default worker `KafkaEx.Server` is used
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
  @spec metadata(Keyword.t) :: map
  def metadata(opts \\ []) do
    worker_name  = Keyword.get(opts, :worker_name, KafkaEx.Server)
    topic = Keyword.get(opts, :topic, "")
    GenServer.call(worker_name, {:metadata, topic})
  end

  @spec consumer_group_metadata(atom, binary) :: KafkaEx.Protocol.ConsumerMetadata.Response.t
  def consumer_group_metadata(worker_name, consumer_group) do
    GenServer.call(worker_name, {:consumer_group_metadata, consumer_group})
  end

  @doc """
  Get the offset of the latest message written to Kafka

  ## Example

  ```elixir
  iex> KafkaEx.latest_offset("foo", 0)
  [%KafkaEx.Protocol.Offset.Response{partition_offsets: [%{error_code: 0, offsets: [16], partition: 0}], topic: "foo"}]
  ```
  """
  @spec latest_offset(binary, integer, atom|pid) :: {atom, map}
  def latest_offset(topic, partition, name \\ KafkaEx.Server), do: offset(topic, partition, :latest, name)

  @doc """
  Get the offset of the earliest message still persistent in Kafka

  ## Example

  ```elixir
  iex> KafkaEx.earliest_offset("foo", 0)
  [%KafkaEx.Protocol.Offset.Response{partition_offsets: [%{error_code: 0, offset: [0], partition: 0}], topic: "foo"}]
  ```
  """
  @spec earliest_offset(binary, integer, atom|pid) :: {atom, map}
  def earliest_offset(topic, partition, name \\ KafkaEx.Server), do: offset(topic, partition, :earliest, name)

  @doc """
  Get the offset of the message sent at the specified date/time

  ## Example

  ```elixir
  iex> KafkaEx.offset("foo", 0, {{2015, 3, 29}, {23, 56, 40}}) # Note that the time specified should match/be ahead of time on the server that kafka runs
  [%KafkaEx.Protocol.Offset.Response{partition_offsets: [%{error_code: 0, offset: [256], partition: 0}], topic: "foo"}]
  ```
  """
  @spec offset(binary, number, :calendar.datetime|atom, atom|pid) :: {atom, map}
  def offset(topic, partition, time, name \\ KafkaEx.Server) do
    GenServer.call(name, {:offset, topic, partition, time})
  end

  @wait_time 10
  @min_bytes 1
  @max_bytes 1_000_000

  @doc """
  Fetch a set of messages from Kafka from the given topic and partition ID

  Optional arguments(KeywordList)
  - offset: When supplied the fetch would start from this offset, otherwise would start from the last committed offset of the consumer_group the worker belongs to. For Kafka < 0.8.2 you should explicitly specify this.
  - worker_name: the worker we want to run this fetch request through. Default is KafkaEx.Server
  - wait_time: maximum amount of time in milliseconds to block waiting if insufficient data is available at the time the request is issued. Default is 10
  - min_bytes: minimum number of bytes of messages that must be available to give a response. If the client sets this to 0 the server will always respond immediately, however if there is no new data since their last request they will just get back empty message sets. If this is set to 1, the server will respond as soon as at least one partition has at least 1 byte of data or the specified timeout occurs. By setting higher values in combination with the timeout the consumer can tune for throughput and trade a little additional latency for reading only large chunks of data (e.g. setting wait_time to 100 and setting min_bytes 64000 would allow the server to wait up to 100ms to try to accumulate 64k of data before responding). Default is 1
  - max_bytes: maximum bytes to include in the message set for this partition. This helps bound the size of the response. Default is 1,000,000
  - auto_commit: specifies if the last offset should be commited or not. Default is true. For Kafka < 0.8.2 set this to false.

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
  @spec fetch(binary, number, Keyword.t) :: {atom, map}
  def fetch(topic, partition, opts \\ []) do
    worker_name   = Keyword.get(opts, :worker_name, KafkaEx.Server)
    offset        = Keyword.get(opts, :offset)
    wait_time     = Keyword.get(opts, :wait_time, @wait_time)
    min_bytes     = Keyword.get(opts, :min_bytes, @min_bytes)
    max_bytes     = Keyword.get(opts, :max_bytes, @max_bytes)
    auto_commit   = Keyword.get(opts, :auto_commit, true)

    offset = case offset do
      nil -> last_offset = offset_fetch(worker_name, %KafkaEx.Protocol.OffsetFetch.Request{topic: topic, partition: partition}) |> KafkaEx.Protocol.OffsetFetch.Response.last_offset
        if last_offset <= 0 do
          0
        else
          last_offset + 1
        end
      _   -> offset
    end

    GenServer.call(worker_name, {:fetch, topic, partition, offset, wait_time, min_bytes, max_bytes, auto_commit})
  end

  @spec offset_commit(atom, KafkaEx.Protocol.OffsetCommit.Request.t) :: KafkaEx.Protocol.OffsetCommit.Response.t
  def offset_commit(worker_name, offset_commit_request) do
    GenServer.call(worker_name, {:offset_commit, offset_commit_request})
  end

  @spec offset_fetch(atom, KafkaEx.Protocol.OffsetFetch.Request.t) :: KafkaEx.Protocol.OffsetFetch.Response.t
  def offset_fetch(worker_name, offset_fetch_request) do
    GenServer.call(worker_name, {:offset_fetch, offset_fetch_request})
  end

  @doc """
  Produces batch messages to kafka logs

  Optional arguments(KeywordList)
  - worker_name: the worker we want to run this metadata request through, when none is provided the default worker `KafkaEx.Server` is used
  ## Example

  ```elixir
  iex> KafkaEx.produce(%KafkaEx.Protocol.Produce.Request{topic: "foo", partition: 0, required_acks: 1, messages: [%KafkaEx.Protocol.Produce.Message{value: "hey"}]})
  :ok
  iex> KafkaEx.produce(%KafkaEx.Protocol.Produce.Request{topic: "foo", partition: 0, required_acks: 1, messages: [%KafkaEx.Protocol.Produce.Message{value: "hey"}]}, worker_name: :pr)
  [%KafkaEx.Protocol.Produce.Response{partitions: [%{error_code: 0, offset: 75, partition: 0}], topic: "foo"}]
  ```
  """
  @spec produce(KafkaEx.Protocol.Produce.Request.t, Keyword.t) :: :ok|list
  def produce(produce_request, opts \\ []) do
    worker_name   = Keyword.get(opts, :worker_name, KafkaEx.Server)
    GenServer.call(worker_name, {:produce, produce_request})
  end

  @doc """
  Produces messages to kafka logs (this is deprecated, use KafkaEx.produce/2 instead)
  Optional arguments(KeywordList)
  - worker_name: the worker we want to run this metadata request through, when none is provided the default worker `KafkaEx.Server` is used
  - key: is used for partition assignment, can be nil, when none is provided it is defaulted to nil
  - require_acks: indicates how many acknowledgements the servers should receive before responding to the request. If it is 0 the server will not send any response (this is the only case where the server will not reply to a request). If it is 1, the server will wait the data is written to the local log before sending a response. If it is -1 the server will block until the message is committed by all in sync replicas before sending a response. For any number > 1 the server will block waiting for this number of acknowledgements to occur (but the server will never wait for more acknowledgements than there are in-sync replicas), default is 0
  - timeout: provides a maximum time in milliseconds the server can await the receipt of the number of acknowledgements in RequiredAcks, default is 100 milliseconds
  - compression: specifies the compression type (:none, :snappy, :gzip)
  ## Example
  ```elixir
  iex> KafkaEx.produce("bar", 0, "hey")
  :ok
  iex> KafkaEx.produce("foo", 0, "hey", [worker_name: :pr, require_acks: 1])
  [%KafkaEx.Protocol.Produce.Response{partitions: [%{error_code: 0, offset: 75, partition: 0}], topic: "foo"}]
  ```
  """
  @spec produce(binary, number, binary, Keyword.t) :: :ok|list
  def produce(topic, partition, value, opts \\ []) do
    key             = Keyword.get(opts, :key, "")
    required_acks   = Keyword.get(opts, :required_acks, 0)
    timeout         = Keyword.get(opts, :timeout, 100)
    produce_request = %KafkaEx.Protocol.Produce.Request{topic: topic, partition: partition, required_acks: required_acks, timeout: timeout, compression: :none, messages: [%KafkaEx.Protocol.Produce.Message{key: key, value: value}]}

    produce(produce_request, opts)
  end

  @doc """
  Returns a stream that consumes fetched messages.
  This puts the specified worker in streaming mode and blocks the worker indefinitely.
  The handler is a normal GenEvent handler so you can supply a custom handler, otherwise a default handler is used.

  This function should be used with care as the queue is unbounded and can cause OOM.

  Optional arguments(KeywordList)
  - worker_name: the worker we want to run this metadata request through, when none is provided the default worker `KafkaEx.Server` is used
  - offset: When supplied the fetch would start from this offset, otherwise would start from the last committed offset of the consumer_group the worker belongs to. For Kafka < 0.8.2 you should explicitly specify this.
  - handler: the handler we want to handle the streaming events, when none is provided the default KafkaExHandler is used
  - handler_init: initial state for the handler - leave the default value [] when using the default handler
  - auto_commit: specifies if the last offset should be commited or not. Default is true. For Kafka < 0.8.2 set this to false.


  ## Example

  ```elixir
  iex> KafkaEx.create_worker(:stream, [{"localhost", 9092}])
  {:ok, #PID<0.196.0>}
  iex> KafkaEx.produce("foo", 0, "hey", worker_name: :stream)
  :ok
  iex> KafkaEx.produce("foo", 0, "hi", worker_name: :stream)
  :ok
  iex> KafkaEx.stream("foo", 0) |> Enum.take(2)
  [%{attributes: 0, crc: 4264455069, key: nil, offset: 0, value: "hey"},
   %{attributes: 0, crc: 4251893211, key: nil, offset: 1, value: "hi"}]
  ```
  """
  @spec stream(binary, number, Keyword.t) :: GenEvent.Stream.t
  def stream(topic, partition, opts \\ []) do
    worker_name  = Keyword.get(opts, :worker_name, KafkaEx.Server)
    offset       = Keyword.get(opts, :offset)
    handler      = Keyword.get(opts, :handler, KafkaExHandler)
    handler_init = Keyword.get(opts, :handler_init, [])
    auto_commit  = Keyword.get(opts, :auto_commit, true)

    offset = case offset do
      nil -> last_offset = offset_fetch(worker_name, %KafkaEx.Protocol.OffsetFetch.Request{topic: topic, partition: partition}) |> KafkaEx.Protocol.OffsetFetch.Response.last_offset
        if last_offset <= 0 do
          0
        else
          last_offset + 1
        end
      _   -> offset
    end

    stream      = GenServer.call(worker_name, {:create_stream, handler, handler_init})
    send(worker_name, {:start_streaming, topic, partition, offset, handler, auto_commit})
    stream
  end

  @spec stop_streaming(Keyword.t) :: :ok
  def stop_streaming(opts \\ []) do
    worker_name = Keyword.get(opts, :worker_name, KafkaEx.Server)
    send(worker_name, :stop_streaming)
  end

#OTP API
  def start(_type, _args) do
    {:ok, pid}     = KafkaEx.Supervisor.start_link
    uris           = Application.get_env(:kafka_ex, :brokers)
    consumer_group = Application.get_env(:kafka_ex, :consumer_group)
    worker_init    = case consumer_group do
      nil            -> [uris: uris]
      consumer_group -> [uris: uris, consumer_group: consumer_group]
    end

    case KafkaEx.create_worker(KafkaEx.Server, worker_init) do
      {:error, reason} -> {:error, reason}
      {:ok, _}         -> {:ok, pid}
    end
  end
end
