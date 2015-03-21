defmodule KafkaEx do
  use Application

  @type datetime() :: {{pos_integer, pos_integer, pos_integer}, {pos_integer, pos_integer, pos_integer}}
  @type uri() :: [{binary|char_list, number}]


  @doc """
  create_worker creates KafkaEx workers with broker list supplied in config

  ## Example

  ```elixir
  iex> KafkaEx.create_worker(:pr)
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
  iex> KafkaEx.create_worker(:pr)
  iex> KafkaEx.create_worker([{"localhost", 9092}], :pr)
  {:ok, #PID<0.171.0>}
  ```
  """
  @spec create_worker(KafkaEx.uri, atom) :: Supervisor.on_start_child
  def create_worker(uris, name) do
    Supervisor.start_child(KafkaEx.Supervisor, [uris, name])
  end

  @doc """
  Return metadata for the given topic; return for all topics if topic is empty string

  ## Example

  ```elixir
  iex> KafkaEx.create_worker(:pr)
  iex> KafkaEx.metadata("foo", :mt)
  %{brokers: %{1 => {"localhost", 9092}},
    topics: %{"foo" => %{error_code: 0,
        partitions: %{0 => %{error_code: 0, isrs: [1], leader: 1, replicas: [1]},
          1 => %{error_code: 0, isrs: [1], leader: 1, replicas: [1]},
          2 => %{error_code: 0, isrs: [1], leader: 1, replicas: [1]},
          3 => %{error_code: 0, isrs: [1], leader: 1, replicas: [1]},
          4 => %{error_code: 0, isrs: [1], leader: 1, replicas: [1]}}}}}
  ```
  """
  def metadata(topic \\ "", name \\ KafkaEx.Server) do
    GenServer.call(name, {:metadata, topic})
  end

  @doc """
  Get the offset of the latest message written to Kafka

  ## Example

  ```elixir
  iex> KafkaEx.latest_offset("foo", 0, :mt)
  {:ok, %{"foo" => %{0 => %{error_code: 0, offsets: [16]}}}}
  ```
  """
  def latest_offset(topic, partition, name \\ KafkaEx.Server), do: offset(topic, partition, :latest, name)

  @doc """
  Get the offset of the earliest message still persistent in Kafka

  ## Example

  ```elixir
  iex> KafkaEx.latest_offset("foo", 0, :mt)
  {:ok, %{"foo" => %{0 => %{error_code: 0, offsets: [0]}}}}
  ```
  """
  def earliest_offset(topic, partition, name \\ KafkaEx.Server), do: offset(topic, partition, :earliest, name)

  @doc """
  Get the offset of the message sent at the specified date/time
  """
  @spec offset(binary, number, datetime|atom) :: map
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
  iex> KafkaEx.fetch("food", 0, 0)
  {:ok,
   %{"food" => %{0 => %{error_code: 0, hw_mark_offset: 133,
         message_set: [%{attributes: 0, crc: 4264455069, key: nil, offset: 0,
            value: "hey"},
          %{attributes: 0, crc: 4264455069, key: nil, offset: 1, value: "hey"},
  ...]}}}}
  ```
  """
  @spec fetch(binary, number, number, atom, number, number, number) :: any
  def fetch(topic, partition, offset, name \\ KafkaEx.Server, wait_time \\ @wait_time, min_bytes \\ @min_bytes, max_bytes \\ @max_bytes) do
    GenServer.call(name, {:fetch, topic, partition, offset, wait_time, min_bytes, max_bytes})
  end

  @doc """
  Produces messages to kafka logs

  ## Example

  ```elixir
  iex> KafkaEx.produce("food", 0, 0)
  :ok
  iex> KafkaEx.produce("foo", 0, "hey", :pr, nil, 1)
  {:ok, %{"foo" => %{0 => %{error_code: 0, offset: 15}}}}
  ```
  """
  @spec produce(binary, number, binary, atom, nil | binary, number, number) :: any
  def produce(topic, partition, value, name \\ KafkaEx.Server, key \\ nil, required_acks \\ 0, timeout \\ 100) do
    GenServer.call(name, {:produce, topic, partition, value, key, required_acks, timeout})
  end

  @doc """
  Returns a stream that consumes fetched messages.
  This puts the specified worker in streaming mode and blocks the worker indefinitely.
  The handler is a normal GenEvent handler so you can supply a custom handler, otherwise a default handler is used.

  This function should be used with care as the queue is unbounded and can cause OOM.

  ## Example

  ```elixir
  iex> KafkaEx.create_worker([{"localhost", 9092}], :stream)
  {:ok, #PID<0.196.0>}
  iex> KafkaEx.produce("foo", 0, "hey", :stream)
  :ok
  iex> KafkaEx.produce("foo", 0, "hi", :stream)
  :ok
  iex> KafkaEx.stream("foo", 0) |> iex> Enum.take(2)
  [%{attributes: 0, crc: 4264455069, key: nil, offset: 0, value: "hey"},
   %{attributes: 0, crc: 4251893211, key: nil, offset: 1, value: "hi"}]
  ```
  """
  @spec stream(binary, number, atom, number, atom) :: GenEvent.Stream.t
  def stream(topic, partition, name \\ KafkaEx.Server, offset \\ 0, handler \\ KafkaExHandler) do
    {:ok, pid} = GenEvent.start_link
    :ok = GenEvent.add_handler(pid, handler, [])
    send(name, {:start_streaming, topic, partition, offset, pid, handler})
    GenEvent.stream(pid)
  end

#OTP API
  def start(_type, _args) do
    {:ok, pid} = KafkaEx.Supervisor.start_link
    uris = Application.get_env(KafkaEx, :brokers)
    case KafkaEx.create_worker(uris, KafkaEx.Server) do
      {:error, reason} -> {:error, reason}
      {:ok, _}         -> {:ok, pid}
    end
  end
end
