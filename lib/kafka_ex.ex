defmodule KafkaEx do
  use Application

  @type uri() :: [{binary|char_list, number}]

  @doc """
  create_worker creates KafkaEx workers with broker list supplied in config

  ## Example

  ```elixir
  iex> KafkaEx.create_worker(:pr) # where :pr is the name of the worker created
  {:ok, #PID<0.171.0>}
  ```
  """
  @spec create_worker(atom) :: Supervisor.on_start_child
  def create_worker(name) do
    uris = Application.get_env(KafkaEx, :brokers)
    Supervisor.start_child(KafkaEx.Supervisor, [uris, name])
  end

  @doc """
  create_worker creates KafkaEx workers

  ## Example

  ```elixir
  iex> KafkaEx.create_worker(:pr, [{"localhost", 9092}])
  {:ok, #PID<0.171.0>}
  ```
  """
  @spec create_worker(atom, KafkaEx.uri) :: Supervisor.on_start_child
  def create_worker(name, uris) do
    Supervisor.start_child(KafkaEx.Supervisor, [uris, name])
  end

  @doc """
  Return metadata for the given topic; returns for all topics if topic is empty string

  ## Example

  ```elixir
  iex> KafkaEx.create_worker(:mt)
  iex> KafkaEx.metadata(topic: "foo", worker_name: :mt)
  %{brokers: %{1 => {"localhost", 9092}},
    topics: %{"foo" => %{error_code: 0,
        partitions: %{0 => %{error_code: 0, isrs: [1], leader: 1, replicas: [1]},
          1 => %{error_code: 0, isrs: [1], leader: 1, replicas: [1]},
          2 => %{error_code: 0, isrs: [1], leader: 1, replicas: [1]},
          3 => %{error_code: 0, isrs: [1], leader: 1, replicas: [1]},
          4 => %{error_code: 0, isrs: [1], leader: 1, replicas: [1]}}}}}
  ```
  """
  @spec metadata(Keyword.t) :: map
  def metadata(opts \\ []) do
    name  = Keyword.get(opts, :worker_name, KafkaEx.Server)
    topic = Keyword.get(opts, :topic, "")
    GenServer.call(name, {:metadata, topic})
  end

  @doc """
  Get the offset of the latest message written to Kafka

  ## Example

  ```elixir
  iex> KafkaEx.latest_offset("foo", 0)
  {:ok, %{"foo" => %{0 => %{error_code: 0, offsets: [16]}}}}
  ```
  """
  @spec latest_offset(binary, integer, atom|pid) :: {atom, map}
  def latest_offset(topic, partition, name \\ KafkaEx.Server), do: offset(topic, partition, :latest, name)

  @doc """
  Get the offset of the earliest message still persistent in Kafka

  ## Example

  ```elixir
  iex> KafkaEx.earliest_offset("foo", 0)
  {:ok, %{"foo" => %{0 => %{error_code: 0, offsets: [0]}}}}
  ```
  """
  @spec earliest_offset(binary, integer, atom|pid) :: {atom, map}
  def earliest_offset(topic, partition, name \\ KafkaEx.Server), do: offset(topic, partition, :earliest, name)

  @doc """
  Get the offset of the message sent at the specified date/time
  """
  @spec offset(binary, number, :calendar.datetime|atom, atom|pid) :: {atom, map}
  def offset(topic, partition, time, name \\ KafkaEx.Server) do
    GenServer.call(name, {:offset, topic, partition, time})
  end

  @wait_time 10
  @min_bytes 1
  @max_bytes 1_000_000

  @doc """
  Fetch a set of messages from Kafka from the given topic, partition ID, and offset

  ## Example

  ```elixir
  iex> KafkaEx.fetch("foo", 0, 0)
  {:ok,
   %{"food" => %{0 => %{error_code: 0, hw_mark_offset: 133,
         message_set: [%{attributes: 0, crc: 4264455069, key: nil, offset: 0,
            value: "hey"},
          %{attributes: 0, crc: 4264455069, key: nil, offset: 1, value: "hey"},
  ...]}}}}
  ```
  """
  @spec fetch(binary, number, number, Keyword.t) :: {atom, map}
  def fetch(topic, partition, offset, opts \\ []) do
    worker_name = Keyword.get(opts, :worker_name, KafkaEx.Server)
    wait_time   = Keyword.get(opts, :wait_time, @wait_time)
    min_bytes   = Keyword.get(opts, :min_bytes, @min_bytes)
    max_bytes   = Keyword.get(opts, :max_bytes, @max_bytes)
    GenServer.call(worker_name, {:fetch, topic, partition, offset, wait_time, min_bytes, max_bytes})
  end

  @doc """
  Produces messages to kafka logs

  ## Example

  ```elixir
  iex> KafkaEx.produce("bar", 0, "hey")
  :ok
  iex> KafkaEx.produce("foo", 0, "hey", [worker_name: :pr, require_acks: 1])
  {:ok, %{"foo" => %{0 => %{error_code: 0, offset: 15}}}}
  ```
  """
  @spec produce(binary, number, binary, Keyword.t) :: :ok|{:ok, map}
  def produce(topic, partition, value, opts \\ []) do
    worker_name   = Keyword.get(opts, :worker_name, KafkaEx.Server)
    key           = Keyword.get(opts, :key, nil)
    required_acks = Keyword.get(opts, :required_acks, 0)
    timeout       = Keyword.get(opts, :timeout, 100)

    GenServer.call(worker_name, {:produce, topic, partition, value, key, required_acks, timeout})
  end

  @doc """
  Returns a stream that consumes fetched messages.
  This puts the specified worker in streaming mode and blocks the worker indefinitely.
  The handler is a normal GenEvent handler so you can supply a custom handler, otherwise a default handler is used.

  This function should be used with care as the queue is unbounded and can cause OOM.

  ## Example

  ```elixir
  iex> KafkaEx.create_worker(:stream, [{"localhost", 9092}])
  {:ok, #PID<0.196.0>}
  iex> KafkaEx.produce("foo", 0, "hey", worker_name: :stream)
  :ok
  iex> KafkaEx.produce("foo", 0, "hi", worker_name: :stream)
  :ok
  iex> KafkaEx.stream("foo", 0) |> iex> Enum.take(2)
  [%{attributes: 0, crc: 4264455069, key: nil, offset: 0, value: "hey"},
   %{attributes: 0, crc: 4251893211, key: nil, offset: 1, value: "hi"}]
  ```
  """
  @spec stream(binary, number, Keyword.t) :: GenEvent.Stream.t
  def stream(topic, partition, opts \\ []) do
    worker_name = Keyword.get(opts, :worker_name, KafkaEx.Server)
    offset      = Keyword.get(opts, :offset, 0)
    handler     = Keyword.get(opts, :handler, KafkaExHandler)

    {:ok, pid}  = GenEvent.start_link
    :ok         = GenEvent.add_handler(pid, handler, [])
    send(worker_name, {:start_streaming, topic, partition, offset, pid, handler})
    GenEvent.stream(pid)
  end

#OTP API
  def start(_type, _args) do
    {:ok, pid} = KafkaEx.Supervisor.start_link
    uris       = Application.get_env(KafkaEx, :brokers)
    case KafkaEx.create_worker(KafkaEx.Server, uris) do
      {:error, reason} -> {:error, reason}
      {:ok, _}         -> {:ok, pid}
    end
  end
end
