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

  `KafkaEx.GenConsumer` manages a consumer's offsets by committing the offsets
  of consumed messages.  KafkaEx supports two commit strategies: asynchronous
  and synchronous.  The return value of `c:handle_message_set/2` determines
  which strategy is used:

  * `{:sync_commit, new_state}` causes synchronous offset commits.
  * `{:async_commit, new_state}` causes asynchronous offset commits.

  Note that with both of the offset commit strategies, only if the final offset
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

  ## Handler state and interaction

  Use the `c:init/2` to initialize consumer state and `c:handle_call/3`,
  `c:handle_cast/2`, or `c:handle_info/2` to interact.

  Example:

  ```
  defmodule MyConsumer do
    use KafkaEx.GenConsumer

    defmodule State do
      defstruct messages: [], calls: 0
    end

    def init(_topic, _partition) do
      {:ok, %State{}}
    end

    def init(_topic, _partition, extra_args) do
      {:ok, %State{}}
    end

    def handle_message_set(message_set, state) do
      {:async_commit, %{state | messages: state.messages ++ message_set}}
    end

    def handle_call(:messages, _from, state) do
      {:reply, state.messages, %{state | calls: state.calls + 1}}
    end
  end

  {:ok, pid} = GenConsumer.start_link(MyConsumer, "consumer_group", "topic", 0)
  GenConsumer.call(pid, :messages)
  ```

  **NOTE** If you do not implement `c:handle_call/3` or `c:handle_cast/2`, any
  calls to `GenConsumer.call/3` or casts to `GenConsumer.cast/2` will raise an
  error. Similarly, any messages sent to a `GenConsumer` will log an error if
  there is no corresponding `c:handle_info/2` callback defined.

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
  Option values used when starting a `KafkaEx.GenConsumer`.
  """
  @type option ::
          {:commit_interval, non_neg_integer}
          | {:commit_threshold, non_neg_integer}
          | {:auto_offset_reset, :none | :earliest | :latest}
          | {:api_versions, map()}
          | {:extra_consumer_args, map()}

  @typedoc """
  Options used when starting a `KafkaEx.GenConsumer`.
  """
  @type options :: [option | GenServer.option()]

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
  @callback init(topic :: binary, partition :: non_neg_integer) ::
              {:ok, state :: term}
              | {:stop, reason :: term}

  @doc """
  Invoked when the server is started. `start_link/5` will block until it
  returns.

  `topic` and `partition` are the arguments passed to `start_link/5`. They
  identify the Kafka partition that the `KafkaEx.GenConsumer` will consume from.

  `extra_args` is the value of the `extra_consumer_args` option to `start_link/5`.

  The default implementation of this function calls `init/2`.

  Returning `{:ok, state}` will cause `start_link/5` to return `{:ok, pid}` and
  the process to start consuming from its assigned partition. `state` becomes
  the consumer's state.

  Any other return value will cause the `start_link/5` to return `{:error,
  error}` and the process to exit.
  """
  @callback init(
              topic :: binary,
              partition :: non_neg_integer,
              extra_args :: map()
            ) :: {:ok, state :: term} | {:stop, reason :: term}

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
  @callback handle_message_set(message_set :: [Message.t()], state :: term) ::
              {:async_commit, new_state :: term}
              | {:sync_commit, new_state :: term}

  @doc """
  Invoked by `KafkaEx.GenConsumer.call/3`.

  Note the default implementation will cause a `RuntimeError`.  If you want to
  interact with your consumer, you must implement a handle_call function.
  """
  @callback handle_call(call :: term, from :: GenServer.from(), state :: term) ::
              {:reply, reply_value :: term, new_state :: term}
              | {:stop, reason :: term, reply_value :: term, new_state :: term}
              | {:stop, reason :: term, new_state :: term}

  @doc """
  Invoked by `KafkaEx.GenConsumer.cast/2`.

  Note the default implementation will cause a `RuntimeError`.  If you want to
  interact with your consumer, you must implement a handle_cast function.
  """
  @callback handle_cast(cast :: term, state :: term) ::
              {:noreply, new_state :: term}
              | {:stop, reason :: term, new_state :: term}

  @doc """
  Invoked by sending messages to the consumer.

  Note the default implementation will log error messages.  If you want to
  interact with your consumer, you must implement a handle_info function.
  """
  @callback handle_info(info :: term, state :: term) ::
              {:noreply, new_state :: term}
              | {:stop, reason :: term, new_state :: term}

  defmacro __using__(_opts) do
    quote do
      @behaviour KafkaEx.GenConsumer
      alias KafkaEx.Protocol.Fetch.Message

      def init(_topic, _partition) do
        {:ok, nil}
      end

      def init(topic, partition, _extra_args) do
        init(topic, partition)
      end

      def handle_call(msg, _from, consumer_state) do
        # taken from the GenServer handle_call implementation
        proc =
          case Process.info(self(), :registered_name) do
            {_, []} -> self()
            {_, name} -> name
          end

        # We do this to trick Dialyzer to not complain about non-local returns.
        case :erlang.phash2(1, 1) do
          0 ->
            raise "attempted to call KafkaEx.GenConsumer #{inspect(proc)} " <>
                    "but no handle_call/3 clause was provided"

          1 ->
            {:reply, {:bad_call, msg}, consumer_state}
        end
      end

      def handle_cast(msg, consumer_state) do
        # taken from the GenServer handle_cast implementation
        proc =
          case Process.info(self(), :registered_name) do
            {_, []} -> self()
            {_, name} -> name
          end

        # We do this to trick Dialyzer to not complain about non-local returns.
        case :erlang.phash2(1, 1) do
          0 ->
            raise "attempted to cast KafkaEx.GenConsumer #{inspect(proc)} " <>
                    " but no handle_cast/2 clause was provided"

          1 ->
            {:noreply, consumer_state}
        end
      end

      def handle_info(msg, consumer_state) do
        # taken from the GenServer handle_info implementation
        proc =
          case Process.info(self(), :registered_name) do
            {_, []} -> self()
            {_, name} -> name
          end

        pattern = '~p ~p received unexpected message in handle_info/2: ~p~n'
        :error_logger.error_msg(pattern, [__MODULE__, proc, msg])
        {:noreply, consumer_state}
      end

      defoverridable init: 2,
                     init: 3,
                     handle_call: 3,
                     handle_cast: 2,
                     handle_info: 2
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
      :member_id,
      :generation_id,
      :current_offset,
      :committed_offset,
      :acked_offset,
      :last_commit,
      :auto_offset_reset,
      :fetch_options,
      :api_versions
    ]
  end

  @commit_interval 5_000
  @commit_threshold 100
  @auto_offset_reset :none

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

  * `:auto_offset_reset` - The policy for resetting offsets when an
    `:offset_out_of_range` error occurs. `:earliest` will move the offset to
    the oldest available, `:latest` moves to the most recent. If anything else
    is specified, the error will simply be raised.

  * `:fetch_options` - Optional keyword list that is passed along to the
    `KafkaEx.fetch` call.

  * `:extra_consumer_args` - Optional parameter that is passed along to the
    `GenConsumer.init` call in the consumer module. Note that if `init/3` is not
    implemented, the default implementation calls to `init/2`, dropping the extra
    arguments.

  Both `:commit_interval` and `:commit_threshold` default to the application
  config (e.g., `Application.get_env/2`) if that value is present, or the
  stated default if the application config is not present.

  Any valid options for `GenServer.start_link/3` can also be specified.

  ### Return Values

  This function has the same return values as `GenServer.start_link/3`.
  """
  @spec start_link(
          consumer_module :: module,
          consumer_group_name :: binary,
          topic_name :: binary,
          partition_id :: non_neg_integer,
          options
        ) :: GenServer.on_start()
  def start_link(consumer_module, group_name, topic, partition, opts \\ []) do
    {server_opts, consumer_opts} =
      Keyword.split(opts, [:debug, :name, :timeout, :spawn_opt])

    GenServer.start_link(
      __MODULE__,
      {consumer_module, group_name, topic, partition, consumer_opts},
      server_opts
    )
  end

  @doc """
  Returns the topic and partition id for this consumer process
  """
  @spec partition(GenServer.server()) ::
          {topic :: binary, partition_id :: non_neg_integer}
  def partition(gen_consumer, timeout \\ 5000) do
    GenServer.call(gen_consumer, :partition, timeout)
  end

  @doc """
  Forwards a `GenServer.call/3` to the consumer implementation with the
  consumer's state.

  The implementation must return a `GenServer.call/3`-compatible value of the
  form `{:reply, reply_value, new_consumer_state}`.  The GenConsumer will
  turn this into an immediate timeout, which drives continued message
  consumption.

  See the moduledoc for an example.
  """
  @spec call(GenServer.server(), term, timeout) :: term
  def call(gen_consumer, message, timeout \\ 5000) do
    GenServer.call(gen_consumer, {:consumer_call, message}, timeout)
  end

  @doc """
  Forwards a `GenServer.cast/2` to the consumer implementation with the
  consumer's state.

  The implementation must return a `GenServer.cast/2`-compatible value of the
  form `{:noreply, new_consumer_state}`. The GenConsumer will turn this into an
  immediate timeout, which drives continued message consumption.
  """
  @spec cast(GenServer.server(), term) :: term
  def cast(gen_consumer, message) do
    GenServer.cast(gen_consumer, {:consumer_cast, message})
  end

  # GenServer callbacks

  def init({consumer_module, group_name, topic, partition, opts}) do
    commit_interval =
      Keyword.get(
        opts,
        :commit_interval,
        Application.get_env(:kafka_ex, :commit_interval, @commit_interval)
      )

    commit_threshold =
      Keyword.get(
        opts,
        :commit_threshold,
        Application.get_env(:kafka_ex, :commit_threshold, @commit_threshold)
      )

    auto_offset_reset =
      Keyword.get(
        opts,
        :auto_offset_reset,
        Application.get_env(:kafka_ex, :auto_offset_reset, @auto_offset_reset)
      )

    extra_consumer_args =
      Keyword.get(
        opts,
        :extra_consumer_args
      )

    generation_id = Keyword.get(opts, :generation_id)
    member_id = Keyword.get(opts, :member_id)

    default_api_versions = %{fetch: 0, offset_fetch: 0, offset_commit: 0}
    api_versions = Keyword.get(opts, :api_versions, %{})
    api_versions = Map.merge(default_api_versions, api_versions)

    case consumer_module.init(topic, partition, extra_consumer_args) do
      {:ok, consumer_state} ->
        worker_opts = Keyword.take(opts, [:uris, :use_ssl, :ssl_options])

        {:ok, worker_name} =
          KafkaEx.create_worker(
            :no_name,
            [consumer_group: group_name] ++ worker_opts
          )

        default_fetch_options = [
          auto_commit: false,
          worker_name: worker_name
        ]

        given_fetch_options = Keyword.get(opts, :fetch_options, [])

        fetch_options =
          Keyword.merge(default_fetch_options, given_fetch_options)

        state = %State{
          consumer_module: consumer_module,
          consumer_state: consumer_state,
          commit_interval: commit_interval,
          commit_threshold: commit_threshold,
          auto_offset_reset: auto_offset_reset,
          worker_name: worker_name,
          group: group_name,
          topic: topic,
          partition: partition,
          generation_id: generation_id,
          member_id: member_id,
          fetch_options: fetch_options,
          api_versions: api_versions
        }

        Process.flag(:trap_exit, true)

        {:ok, state, 0}

      {:stop, reason} ->
        {:stop, reason}
    end
  end

  def handle_call(:partition, _from, state) do
    {:reply, {state.topic, state.partition}, state, 0}
  end

  def handle_call(
        {:consumer_call, message},
        from,
        %State{
          consumer_module: consumer_module,
          consumer_state: consumer_state
        } = state
      ) do
    # NOTE we only support the {:reply, _, _} result format here
    #   which we turn into a timeout = 0 clause so that we continue to consume.
    #   any other GenServer flow control could have unintended consequences,
    #   so we leave that for later consideration
    consumer_reply =
      consumer_module.handle_call(
        message,
        from,
        consumer_state
      )

    case consumer_reply do
      {:reply, reply, new_consumer_state} ->
        {:reply, reply, %{state | consumer_state: new_consumer_state}, 0}

      {:stop, reason, new_consumer_state} ->
        {:stop, reason, %{state | consumer_state: new_consumer_state}}

      {:stop, reason, reply, new_consumer_state} ->
        {:stop, reason, reply, %{state | consumer_state: new_consumer_state}}
    end
  end

  def handle_cast(
        {:consumer_cast, message},
        %State{
          consumer_module: consumer_module,
          consumer_state: consumer_state
        } = state
      ) do
    # NOTE we only support the {:noreply, _} result format here
    #   which we turn into a timeout = 0 clause so that we continue to consume.
    #   any other GenServer flow control could have unintended consequences,
    #   so we leave that for later consideration
    consumer_reply =
      consumer_module.handle_cast(
        message,
        consumer_state
      )

    case consumer_reply do
      {:noreply, new_consumer_state} ->
        {:noreply, %{state | consumer_state: new_consumer_state}, 0}

      {:stop, reason, new_consumer_state} ->
        {:stop, reason, %{state | consumer_state: new_consumer_state}}
    end
  end

  def handle_info(
        :timeout,
        %State{current_offset: nil, last_commit: nil} = state
      ) do
    new_state = %State{
      load_offsets(state)
      | last_commit: :erlang.monotonic_time(:milli_seconds)
    }

    {:noreply, new_state, 0}
  end

  def handle_info(:timeout, %State{} = state) do
    case consume(state) do
      {:error, reason} ->
        {:stop, reason, state}

      new_state ->
        {:noreply, new_state, 0}
    end
  end

  def handle_info(
        message,
        %State{
          consumer_module: consumer_module,
          consumer_state: consumer_state
        } = state
      ) do
    # NOTE we only support the {:noreply, _} result format here
    #   which we turn into a timeout = 0 clause so that we continue to consume.
    #   any other GenServer flow control could have unintended consequences,
    #   so we leave that for later consideration
    consumer_reply =
      consumer_module.handle_info(
        message,
        consumer_state
      )

    case consumer_reply do
      {:noreply, new_consumer_state} ->
        {:noreply, %{state | consumer_state: new_consumer_state}, 0}

      {:stop, reason, new_consumer_state} ->
        {:stop, reason, %{state | consumer_state: new_consumer_state}}
    end
  end

  def terminate(_reason, %State{} = state) do
    commit(state)
    Process.unlink(state.worker_name)
    KafkaEx.stop_worker(state.worker_name)
  end

  # Helpers

  defp consume(
         %State{
           topic: topic,
           partition: partition,
           current_offset: offset,
           fetch_options: fetch_options
         } = state
       ) do
    response =
      KafkaEx.fetch(
        topic,
        partition,
        Keyword.merge(
          fetch_options,
          offset: offset,
          api_version: Map.fetch!(state.api_versions, :fetch)
        )
      )

    response
    |> handle_fetch_response(state)
  end

  defp handle_fetch_response(
         [
           %FetchResponse{
             topic: _topic,
             partitions: [
               response = %{error_code: error_code, partition: _partition}
             ]
           }
         ],
         state
       ) do
    state =
      case error_code do
        :offset_out_of_range ->
          handle_offset_out_of_range(state)

        :no_error ->
          state
      end

    case response do
      %{message_set: []} ->
        handle_commit(:async_commit, state)

      %{last_offset: _, message_set: message_set} ->
        handle_message_set(message_set, state)
    end
  end

  defp handle_fetch_response(error, _state) do
    {:error, error}
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
      state
      | consumer_state: new_consumer_state,
        acked_offset: last_offset + 1,
        current_offset: last_offset + 1
    }

    handle_commit(sync_status, state_out)
  end

  defp handle_offset_out_of_range(
         %State{
           worker_name: worker_name,
           topic: topic,
           partition: partition,
           auto_offset_reset: auto_offset_reset
         } = state
       ) do
    [
      %OffsetResponse{
        topic: ^topic,
        partition_offsets: [
          %{partition: ^partition, error_code: :no_error, offset: [offset]}
        ]
      }
    ] =
      case auto_offset_reset do
        :earliest ->
          KafkaEx.earliest_offset(topic, partition, worker_name)

        :latest ->
          KafkaEx.latest_offset(topic, partition, worker_name)

        _ ->
          raise "Offset out of range while consuming topic #{topic}, partition #{
                  partition
                }."
      end

    %State{
      state
      | current_offset: offset,
        committed_offset: offset,
        acked_offset: offset
    }
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

  defp commit(%State{acked_offset: offset, committed_offset: offset} = state) do
    state
  end

  defp commit(
         %State{
           worker_name: worker_name,
           group: group,
           topic: topic,
           partition: partition,
           member_id: member_id,
           generation_id: generation_id,
           acked_offset: offset
         } = state
       ) do
    request = %OffsetCommitRequest{
      consumer_group: group,
      topic: topic,
      partition: partition,
      offset: offset,
      member_id: member_id,
      generation_id: generation_id,
      api_version: Map.fetch!(state.api_versions, :offset_fetch)
    }

    [%OffsetCommitResponse{topic: ^topic, partitions: [partition_response]}] =
      KafkaEx.offset_commit(worker_name, request)

    # one of these needs to match, depending on which client
    case partition_response do
      # old client
      ^partition ->
        :ok

      # new client
      %{error_code: :no_error, partition: ^partition} ->
        :ok
    end

    Logger.debug(fn ->
      "Committed offset #{topic}/#{partition}@#{offset} for #{group}"
    end)

    %State{
      state
      | committed_offset: offset,
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
      partition: partition,
      api_version: Map.fetch!(state.api_versions, :offset_fetch)
    }

    [
      %OffsetFetchResponse{
        topic: ^topic,
        partitions: [
          %{partition: ^partition, error_code: error_code, offset: offset}
        ]
      }
    ] = KafkaEx.offset_fetch(worker_name, request)

    # newer api versions will return -1 if the consumer group does not exist
    offset = max(offset, 0)

    case error_code do
      :no_error ->
        %State{
          state
          | current_offset: offset,
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
          state
          | current_offset: offset,
            committed_offset: offset,
            acked_offset: offset
        }
    end
  end
end
