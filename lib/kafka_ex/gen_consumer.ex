defmodule KafkaEx.GenConsumer do
  @moduledoc """
  A behaviour module for implementing a Kafka consumer.

  A `KafkaEx.GenConsumer` is an Elixir process that consumes messages from
  Kafka. A single `KafkaEx.GenConsumer` process consumes from a single
  partition of a Kafka topic. Several `KafkaEx.GenConsumer` processes can be
  used to consume from multiple partitions or even multiple topics. Partition
  assignments for a group of `KafkaEx.GenConsumer`s can be defined manually
  using `KafkaEx.GenConsumer.Supervisor` or coordinated across a cluster of
  nodes using `KafkaEx.ConsumerGroup`.

  A `KafkaEx.GenConsumer` must implement three callbacks.  Two of these will be
  defined with default behavior if you add `use KafkaEx.GenConsumer` to your
  module, leaving just `c:handle_message_set/2` to be implemented.  This is the
  recommended usage.

  ## Example

  The `KafkaEx.GenConsumer` behaviour abstracts common Kafka consumer
  interactions.  `KafkaEx.GenConsumer` will take care of the details of
  determining a starting offset, fetching messages from a Kafka broker, and
  committing offsets for consumed messages. Developers are only required to
  implement `c:handle_message_set/2` to process messages.

  The following is a minimal example that logs each message as it's consumed:

  ```
  defmodule ExampleGenConsumer do
    use KafkaEx.GenConsumer

    alias KafkaEx.Protocol.Fetch.Message

    require Logger

    # note - messages are delivered in batches
    def handle_message_set(message_set, state) do
      for %Message{value: message} <- message_set do
        Logger.debug(fn -> "message: " <> inspect(message) end)
      end
      {:async_commit, state}
    end
  end
  ```

  `c:handle_message_set/2` will be called with the batch of messages fetched
  from the broker.  The number of messages in a batch is determined by the
  number of messages available and the `max_bytes` and `min_bytes` parameters
  of the fetch request (which can be configured in KafkaEx).  In this example,
  because `c:handle_message_set/2` always returns `{:async_commit, new_state}`,
  the message offsets will be automatically committed asynchronously.

  ## Committing Offsets

  `KafkaEx.GenConsumer` manages a consumer's offsets by committing the the offsets
  of consumed messages.  KafkaEx supports two commit strategies: asynchronous
  and synchronous.  The return value of `c:handle_message_set/2` determines
  which strategy is used:
  
  * `{:sync_commit, new_state}` causes synchronous offset commits.
  * `{:async_commit, new_state}` causes asynchronous offset commits.

  Note that with both of the offset commit strategies, only of the final offset
  in the message set is committed and this is done after the messages are
  consumed.  If you want to commit the offset of every message consumed, use
  the synchronous offset commit strategy and implement calls to
  `KafkaEx.offset_commit/2` within your consumer as appropriate.

  ### Synchronous offset commits

  When `c:handle_message_set/2` returns `{:sync_commit, new_state}`, the offset
  of the final message in the message set is committed immediately before
  fetching any more messages.  This strategy requires a significant amount of
  communication with the broker and could correspondingly degrade consumer
  performance, but it will keep the offset commits tightly synchronized with
  the consumer state.

  Choose the synchronous offset commit strategy if you want to favor
  consistency of offset commits over performance, or if you have a low rate of
  message arrival.  The definition of a "low rate" depends on the situation,
  but tens of messages per second could be considered a "low rate" in most
  situations.

  ### Asynchronous offset commits

  When `c:handle_message_set/2` returns `{:async_commit, new_state}`, KafkaEx
  will not commit offsets after every message set consumed.  To avoid
  excessive network calls, the offsets are committed periodically (and when
  the worker terminates).

  How often a `KafkaEx.GenConsumer` auto-commits offsets is controlled by the two
  configuration values `:commit_interval` and `:commit_threshold`.

  * `:commit_interval` is the maximum time (in milliseconds) that a
    `KafkaEx.GenConsumer` will delay committing the offset for an acknowledged
    message.

  * `:commit_threshold` is the maximum number of acknowledged messages that a
    `KafkaEx.GenConsumer` will allow to be uncommitted before triggering a
    commit.
  
  These can be set globally in the `:kafka_ex` app's environment or on a
  per-consumer basis by passing options to `start_link/5`:

  ```
  # In config/config.exs
  config :kafka_ex,
    commit_interval: 5000,
    commit_threshold: 100

  # As options to start_link/5
  KafkaEx.GenConsumer.start_link(MyConsumer, "my_group", "topic", 0,
                                 commit_interval: 5000,
                                 commit_threshold: 100)
  ```

  For low-volume topics, `:commit_interval` is the dominant factor for how
  often a `KafkaEx.GenConsumer` auto-commits. For high-volume topics,
  `:commit_threshold` is the dominant factor.

  ## Integration with OTP

  A `KafkaEx.GenConsumer` is a specialized `GenServer`. It can be supervised,
  registered, and debugged the same as any other `GenServer`.  Use
  `start_link/5` to start a `KafkaEx.GenConsumer` properly; do not use
  `GenServer.start_link/3` directly to start a `KafkaEx.GenConsumer`.

  ## Testing

  A `KafkaEx.GenConsumer` can be unit-tested without a running Kafka broker by sending
  messages directly to its `c:handle_message_set/2` function. The following
  recipe can be used as a starting point when testing a `KafkaEx.GenConsumer`:

  ```
  defmodule ExampleGenConsumerTest do
    use ExUnit.Case, async: true

    alias KafkaEx.Protocol.Fetch.Message

    @topic "topic"
    @partition 0

    setup do
      {:ok, state} = ExampleGenConsumer.init(@topic, @partition)
      {:ok, %{state: state}}
    end

    test "it acks a message", %{state: state} do
      message_set = [%Message{offset: 0, value: "hello"}]
      {response, _new_state} =
        ExampleGenConsumer.handle_message_set(message_set, state)
      assert response == :async_commit
    end
  end
  ```
  """

  use GenServer

  alias KafkaEx.Protocol.OffsetCommit.Request, as: OffsetCommitRequest
  alias KafkaEx.Protocol.OffsetCommit.Response, as: OffsetCommitResponse
  alias KafkaEx.Protocol.OffsetFetch.Request, as: OffsetFetchRequest
  alias KafkaEx.Protocol.OffsetFetch.Response, as: OffsetFetchResponse
  alias KafkaEx.Protocol.Offset.Response, as: OffsetResponse
  alias KafkaEx.Protocol.Fetch.Response, as: FetchResponse
  alias KafkaEx.Protocol.Fetch.Message

  require Logger

  @typedoc """
  The ID (string) of a member of a consumer group, assigned by a Kafka broker.
  """
  @type member_id :: binary

  @typedoc """
  The string name of a Kafka topic.
  """
  @type topic :: binary

  @typedoc """
  The integer ID of a partition of a Kafka topic.
  """
  @type partition_id :: integer

  @typedoc """
  A partition of a single topic (embeds the name of the topic).
  """
  @type partition :: {topic, partition_id}

  @typedoc """
  Option values used when starting a `KafkaEx.GenConsumer`.
  """
  @type option :: {:commit_interval, non_neg_integer}
                | {:commit_threshold, non_neg_integer}

  @typedoc """
  Options used when starting a `KafkaEx.GenConsumer`.
  """
  @type options :: [option | GenServer.option]

  @doc """
  Invoked when the server is started. `start_link/5` will block until it
  returns.

  `topic` and `partition` are the arguments passed to `start_link/5`. They
  identify the Kafka partition that the `KafkaEx.GenConsumer` will consume from.

  Returning `{:ok, state}` will cause `start_link/5` to return `{:ok, pid}` and
  the process to start consuming from its assigned partition. `state` becomes
  the consumer's state.

  Any other return value will cause the `start_link/5` to return `{:error,
  error}` and the process to exit.
  """
  @callback init(topic :: topic, partition :: partition_id) ::
    {:ok, state :: term}

  @doc """
  Invoked for each message set consumed from a Kafka topic partition.

  `message_set` is a message set fetched from a Kafka broker and `state` is the
  current state of the `KafkaEx.GenConsumer`.

  Returning `{:async_commit, new_state}` acknowledges `message` and continues
  to consume from the Kafka queue with new state `new_state`. Acknowledged
  messages will be auto-committed (possibly at a later time) based on the
  `:commit_interval` and `:commit_threshold` options.

  Returning `{:sync_commit, new_state}` commits `message` synchronously before
  continuing to consume from the Kafka queue with new state `new_state`.
  Committing a message synchronously means that no more messages will be
  consumed until the message's offset is committed. `:sync_commit` should be
  used sparingly, since committing every message synchronously would impact a
  consumer's performance and could result in excessive network traffic.
  """
  @callback handle_message_set(message_set :: [Message.t], state :: term) ::
    {:async_commit, new_state :: term} | {:sync_commit, new_state :: term}

  @doc """
  Invoked to determine partition assignments for a coordinated consumer group.

  `members` is a list of member IDs and `partitions` is a list of partitions
  that need to be assigned to a group member.

  The return value must be a map with member IDs as keys and a list of
  partition assignments as values. For each member ID in the returned map, the
  assigned partitions will become the `assignments` argument to
  `KafkaEx.GenConsumer.Supervisor.start_link/4` in the corresponding member
  process. Any member that's omitted from the return value will not be assigned
  any partitions.

  If this callback is not implemented, the default implementation by `use
  KafkaEx.GenConsumer` implements a simple round-robin assignment.

  ### Example

  Given the following `members` and `partitions` to be assigned:

  ```
  members = ["member1", "member2", "member3"]
  partitions = [{"topic", 0}, {"topic", 1}, {"topic", 2}]
  ```

  One possible assignment is as follows:

  ```
  ExampleGenConsumer.assign_partitions(members, partitions)
  #=> %{"member1" => [{"topic", 0}, {"topic", 2}], "member2" => [{"topic", 1}]}
  ```

  In this case, the consumer group process for `"member1"` will launch two
  `KafkaEx.GenConsumer` processes (one for each of its assigned partitions),
  `"member2"` will launch one `KafkaEx.GenConsumer` process, and `"member3"` will
  launch no processes.
  """
  @callback assign_partitions(
    members :: [member_id],
    partitions :: [partition]
  ) :: %{member_id => [partition]}

  defmacro __using__(_opts) do
    quote do
      @behaviour KafkaEx.GenConsumer
      alias KafkaEx.ConsumerGroup.PartitionAssignment
      alias KafkaEx.Protocol.Fetch.Message

      def init(_topic, _partition) do
        {:ok, nil}
      end

      def assign_partitions(members, partitions) do
        PartitionAssignment.round_robin(members, partitions)
      end

      defoverridable [init: 2, assign_partitions: 2]
    end
  end

  defmodule State do
    @moduledoc false
    defstruct [
      :consumer_module,
      :consumer_state,
      :commit_interval,
      :commit_threshold,
      :worker_name,
      :group,
      :topic,
      :partition,
      :current_offset,
      :committed_offset,
      :acked_offset,
      :last_commit,
    ]
  end

  @commit_interval 5_000
  @commit_threshold 100

  # Client API

  @doc """
  Starts a `KafkaEx.GenConsumer` process linked to the current process.

  This can be used to start the `KafkaEx.GenConsumer` as part of a supervision tree.

  Once the consumer has been started, the `c:init/2` function of
  `consumer_module` is called with `topic` and `partition` as its arguments.
  `group_name` is the consumer group name that will be used for managing
  consumer offsets.

  ### Options

  * `:commit_interval` - The interval in milliseconds that the consumer will
    wait to commit offsets of handled messages.  Default 5_000.

  * `:commit_threshold` - Threshold number of messages consumed to commit
    offsets to the broker.  Default 100.

  Both `:commit_interval` and `:commit_threshold` default to the application
  config (e.g., `Application.get_env/2`) if that value is present, or the
  stated default if the application config is not present.

  Any valid options for `GenServer.start_link/3` can also be specified.

  ### Return Values

  This function has the same return values as `GenServer.start_link/3`.
  """
  @spec start_link(
    module,
    binary,
    topic,
    partition_id,
    options
  ) :: GenServer.on_start
  def start_link(consumer_module, group_name, topic, partition, opts \\ []) do
    {server_opts, consumer_opts} =
      Keyword.split(opts, [:debug, :name, :timeout, :spawn_opt])

    GenServer.start_link(
      __MODULE__,
      {consumer_module, group_name, topic, partition, consumer_opts},
      server_opts
    )
  end

  # GenServer callbacks

  def init({consumer_module, group_name, topic, partition, opts}) do
    commit_interval = Keyword.get(
      opts,
      :commit_interval,
      Application.get_env(:kafka_ex, :commit_interval, @commit_interval)
    )
    commit_threshold = Keyword.get(
      opts,
      :commit_threshold,
      Application.get_env(:kafka_ex, :commit_threshold, @commit_threshold)
    )

    {:ok, consumer_state} = consumer_module.init(topic, partition)
    {:ok, worker_name} =
      KafkaEx.create_worker(:no_name, consumer_group: group_name)

    state = %State{
      consumer_module: consumer_module,
      consumer_state: consumer_state,
      commit_interval: commit_interval,
      commit_threshold: commit_threshold,
      worker_name: worker_name,
      group: group_name,
      topic: topic,
      partition: partition,
    }

    Process.flag(:trap_exit, true)

    {:ok, state, 0}
  end

  def handle_info(
    :timeout,
    %State{current_offset: nil, last_commit: nil} = state
  ) do
    new_state = %State{
      load_offsets(state) |
      last_commit: :erlang.monotonic_time(:milli_seconds)
    }

    {:noreply, new_state, 0}
  end

  def handle_info(:timeout, %State{} = state) do
    new_state = consume(state)

    {:noreply, new_state, 0}
  end

  def terminate(_reason, %State{} = state) do
    commit(state)
  end

  # Helpers

  defp consume(
    %State{
      worker_name: worker_name,
      topic: topic,
      partition: partition,
      current_offset: offset
    } = state
  ) do
    [
      %FetchResponse{
        topic: ^topic,
        partitions: [
          response = %{error_code: :no_error, partition: ^partition}
        ]
      }
    ] = KafkaEx.fetch(
      topic,
      partition,
      offset: offset,
      auto_commit: false,
      worker_name: worker_name
    )

    case response do
      %{last_offset: nil, message_set: []} ->
        handle_commit(:async_commit, state)
      %{last_offset: _, message_set: message_set} ->
        handle_message_set(message_set, state)
    end
  end

  defp handle_message_set(
    message_set,
    %State{
      consumer_module: consumer_module,
      consumer_state: consumer_state
    } = state
  ) do
    {sync_status, new_consumer_state} =
      consumer_module.handle_message_set(message_set, consumer_state)

    %Message{offset: last_offset} = List.last(message_set)
    state_out = %State{
      state |
      consumer_state: new_consumer_state,
      acked_offset: last_offset + 1,
      current_offset: last_offset + 1
    }

    handle_commit(sync_status, state_out)
  end

  defp handle_commit(:sync_commit, %State{} = state), do: commit(state)
  defp handle_commit(
    :async_commit,
    %State{
      acked_offset: acked,
      committed_offset: committed,
      commit_threshold: threshold,
      last_commit: last_commit,
      commit_interval: interval
    } = state
  ) do
    case acked - committed do
      0 ->
        %State{state | last_commit: :erlang.monotonic_time(:milli_seconds)}
      n when n >= threshold ->
        commit(state)
      _ ->
        if :erlang.monotonic_time(:milli_seconds) - last_commit >= interval do
          commit(state)
        else
          state
        end
    end
  end

  defp commit(
    %State{acked_offset: offset, committed_offset: offset} = state
  ) do
    state
  end

  defp commit(
    %State{
      worker_name: worker_name,
      group: group,
      topic: topic,
      partition: partition,
      acked_offset: offset
    } = state
  ) do
    request = %OffsetCommitRequest{
      consumer_group: group,
      topic: topic,
      partition: partition,
      offset: offset,
    }

    [%OffsetCommitResponse{topic: ^topic, partitions: [^partition]}] =
      KafkaEx.offset_commit(worker_name, request)

    Logger.debug(fn ->
      "Committed offset #{topic}/#{partition}@#{offset} for #{group}"
    end)

    %State{
      state |
      committed_offset: offset,
      last_commit: :erlang.monotonic_time(:milli_seconds)
    }
  end

  defp load_offsets(
    %State{
      worker_name: worker_name,
      group: group,
      topic: topic,
      partition: partition
    } = state
  ) do
    request = %OffsetFetchRequest{
      consumer_group: group,
      topic: topic,
      partition: partition
    }

    [
      %OffsetFetchResponse{
        topic: ^topic,
        partitions: [
          %{partition: ^partition, error_code: error_code, offset: offset}
        ]
      }
    ] = KafkaEx.offset_fetch(worker_name, request)

    case error_code do
      :no_error ->
        %State{
          state |
          current_offset: offset,
          committed_offset: offset,
          acked_offset: offset
        }
      :unknown_topic_or_partition ->
        [
          %OffsetResponse{
            topic: ^topic,
            partition_offsets: [
              %{partition: ^partition, error_code: :no_error, offset: [offset]}
            ]
          }
        ] = KafkaEx.earliest_offset(topic, partition, worker_name)

        %State{
          state |
          current_offset: offset,
          committed_offset: offset,
          acked_offset: offset
        }
    end
  end
end
