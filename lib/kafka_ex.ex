defmodule KafkaEx do
  use Application

  @type datetime() :: {{pos_integer, pos_integer, pos_integer}, {pos_integer, pos_integer, pos_integer}}
  @type uri() :: [{binary|char_list, number}]


  @doc """
  create_worker creates KafkaEx workers with broker list supplied in config
  """
  @spec create_worker(atom) :: Supervisor.on_start_child
  def create_worker(name) do
    uris = Application.get_env(KafkaEx, :brokers)
    Supervisor.start_child(KafkaEx.Supervisor, [uris, name])
  end

  @doc """
  create_worker creates KafkaEx workers
  """
  @spec create_worker(KafkaEx.Server.uri, atom) :: Supervisor.on_start_child
  def create_worker(uris, name) do
    Supervisor.start_child(KafkaEx.Supervisor, [uris, name])
  end

  def metadata(topic \\ "", name \\ KafkaEx.Server) do
    GenServer.call(name, {:metadata, topic})
  end

  def latest_offset(topic, partition, name \\ KafkaEx.Server), do: offset(topic, partition, :latest, name)

  def earliest_offset(topic, partition, name \\ KafkaEx.Server), do: offset(topic, partition, :earliest, name)

  @spec offset(binary, number, datetime|atom) :: map
  def offset(topic, partition, time, name \\ KafkaEx.Server) do
    GenServer.call(name, {:offset, topic, partition, time})
  end

  @wait_time 10
  @min_bytes 1
  @max_bytes 1_000_000
  @spec fetch(binary, number, number, atom, number, number, number) :: any
  def fetch(topic, partition, offset, name \\ KafkaEx.Server, wait_time \\ @wait_time, min_bytes \\ @min_bytes, max_bytes \\ @max_bytes) do
    GenServer.call(name, {:fetch, topic, partition, offset, wait_time, min_bytes, max_bytes})
  end

  def produce(topic, partition, value, name \\ KafkaEx.Server, key \\ nil, required_acks \\ 0, timeout \\ 100) do
    GenServer.call(name, {:produce, topic, partition, value, key, required_acks, timeout})
  end

  @doc """
  Returns a stream that consumes fetched messages.
  This puts the specified worker in streaming mode and blocks the worker indefinitely.
  The handler is a normal GenEvent handler so you can supply a custom handler, otherwise a default handler is used.

  This function should be used with care as the queue is unbounded and can cause OOM.

  e.g:

    KafkaEx.Server.create_worker([{"localhost", 9092}], :stream)
    KafkaEx.Server.produce("foo", 0, "hey", :stream)
    KafkaEx.Server.produce("foo", 0, "hi", :stream)
    KafkaEx.Server.start_stream("foo", 0) |>
    Enum.take(2)  #> [%{attributes: 0, crc: 4264455069, key: nil, offset: 0, value: "hey"},
    %{attributes: 0, crc: 4251893211, key: nil, offset: 1, value: "hi"}]
  """
  @spec stream(binary, number, atom, number, atom) :: GenEvent.Stream.t
  def stream(topic, partition, name \\ KafkaEx.Server, offset \\ 0, handler \\ KafkaExHandler) do
    {:ok, pid} = GenEvent.start_link
    :ok = GenEvent.add_handler(pid, handler, [])
    send(name, {:start_streaming, topic, partition, offset, pid, handler})
    GenEvent.stream(pid)
  end

  def start(_type, _args) do
    {:ok, pid} = KafkaEx.Supervisor.start_link
    uris = Application.get_env(KafkaEx, :brokers)
    KafkaEx.create_worker(uris, KafkaEx.Server)
    {:ok, pid}
  end
end
