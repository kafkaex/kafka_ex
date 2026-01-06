defmodule KafkaEx.Telemetry do
  @moduledoc """
  Telemetry events emitted by KafkaEx.

  KafkaEx uses the `:telemetry` library to emit events that can be used
  for observability. This module documents all events and provides
  helper functions.

  ## Attaching Handlers

  To receive telemetry events, attach a handler:

      :telemetry.attach("my-handler", [:kafka_ex, :request, :stop], &MyModule.handle_event/4, nil)

  Or attach to multiple events:

    :telemetry.attach_many("my-handler", KafkaEx.Telemetry.events(), &MyModule.handle_event/4, nil)

  ## Events

  ### Request Events

  Low-level Kafka protocol request/response events.

  * `[:kafka_ex, :request, :start]` - Emitted when a Kafka protocol request begins
    * Measurements: `%{system_time: integer()}`
    * Metadata: `%{operation: atom(), api_version: integer(), correlation_id: integer(),  client_id: binary(), broker: map()}`

  * `[:kafka_ex, :request, :stop]` - Emitted when a Kafka protocol request completes
    * Measurements: `%{duration: integer()}`
    * Metadata: Same as start event

  * `[:kafka_ex, :request, :exception]` - Emitted when a Kafka protocol request fails
    * Measurements: `%{duration: integer()}`
    * Metadata: Start metadata plus `%{kind: atom(), reason: term(), stacktrace: list()}`

  ### Connection Events

  * `[:kafka_ex, :connection, :start]` - Emitted when connecting to a broker
    * Measurements: `%{system_time: integer()}`
    * Metadata: `%{host: binary(), port: integer(), ssl: boolean()}`

  * `[:kafka_ex, :connection, :stop]` - Emitted when connection is established
    * Measurements: `%{duration: integer()}`
    * Metadata: Same as start event plus `%{success: boolean()}`

  * `[:kafka_ex, :connection, :exception]` - Emitted when connection fails with exception
    * Measurements: `%{duration: integer()}`
    * Metadata: Start metadata plus `%{kind: atom(), reason: term(), stacktrace: list()}`

  ### Produce Events

  * `[:kafka_ex, :produce, :start]` - Emitted when a produce operation begins
    * Measurements: `%{system_time: integer(), message_count: integer()}`
    * Metadata: `%{topic: binary(), partition: integer(), client_id: binary(), required_acks: integer()}`

  * `[:kafka_ex, :produce, :stop]` - Emitted when a produce operation completes
    * Measurements: `%{duration: integer()}`
    * Metadata: Same as start event plus `%{result: :ok | :error, offset: integer(), error: term()}`
      * `result` - `:ok` on success, `:error` on failure
      * `offset` - Base offset (only on success when acks > 0)
      * `error` - Error reason (only on failure)

  * `[:kafka_ex, :produce, :exception]` - Emitted when a produce operation fails
    * Measurements: `%{duration: integer()}`
    * Metadata: Start metadata plus `%{kind: atom(), reason: term(), stacktrace: list()}`

  ### Fetch Events

  * `[:kafka_ex, :fetch, :start]` - Emitted when a fetch operation begins
    * Measurements: `%{system_time: integer()}`
    * Metadata: `%{topic: binary(), partition: integer(), offset: integer(), client_id: binary()}`

  * `[:kafka_ex, :fetch, :stop]` - Emitted when a fetch operation completes
    * Measurements: `%{duration: integer()}`
    * Metadata: Same as start event plus `%{result: :ok | :error, message_count: integer(), error: term()}`
      * `result` - `:ok` on success, `:error` on failure
      * `message_count` - Number of messages fetched (only on success)
      * `error` - Error reason (only on failure)

  * `[:kafka_ex, :fetch, :exception]` - Emitted when a fetch operation fails
    * Measurements: `%{duration: integer()}`
    * Metadata: Start metadata plus `%{kind: atom(), reason: term(), stacktrace: list()}`

  ### Consumer Events

  * `[:kafka_ex, :consumer, :commit, :start]` - Emitted when an offset commit begins
    * Measurements: `%{system_time: integer()}`
    * Metadata: `%{group_id: binary(), client_id: binary(), topic: binary(), partition_count: integer()}`

  * `[:kafka_ex, :consumer, :commit, :stop]` - Emitted when an offset commit completes
    * Measurements: `%{duration: integer()}`
    * Metadata: Same as start event plus `%{result: :ok | :error, error: term()}`
      * `result` - `:ok` on success, `:error` on failure
      * `error` - Error reason (only on failure)

  * `[:kafka_ex, :consumer, :commit, :exception]` - Emitted when an offset commit fails
    * Measurements: `%{duration: integer()}`
    * Metadata: Start metadata plus `%{kind: atom(), reason: term(), stacktrace: list()}`
  """

  @protocol Application.compile_env(:kafka_ex, :protocol, KafkaEx.Protocol.KayrockProtocol)

  @request_start [:kafka_ex, :request, :start]
  @request_stop [:kafka_ex, :request, :stop]
  @request_exception [:kafka_ex, :request, :exception]

  @connection_start [:kafka_ex, :connection, :start]
  @connection_stop [:kafka_ex, :connection, :stop]
  @connection_exception [:kafka_ex, :connection, :exception]

  @produce_start [:kafka_ex, :produce, :start]
  @produce_stop [:kafka_ex, :produce, :stop]
  @produce_exception [:kafka_ex, :produce, :exception]

  @fetch_start [:kafka_ex, :fetch, :start]
  @fetch_stop [:kafka_ex, :fetch, :stop]
  @fetch_exception [:kafka_ex, :fetch, :exception]

  @consumer_commit_start [:kafka_ex, :consumer, :commit, :start]
  @consumer_commit_stop [:kafka_ex, :consumer, :commit, :stop]
  @consumer_commit_exception [:kafka_ex, :consumer, :commit, :exception]

  @request_events [@request_start, @request_stop, @request_exception]
  @connection_events [@connection_start, @connection_stop, @connection_exception]
  @produce_events [@produce_start, @produce_stop, @produce_exception]
  @fetch_events [@fetch_start, @fetch_stop, @fetch_exception]
  @consumer_events [@consumer_commit_start, @consumer_commit_stop, @consumer_commit_exception]

  @doc """
  Returns the list of all telemetry events emitted by KafkaEx.
  """
  @spec events() :: [list(atom())]
  def events do
    @request_events ++ @connection_events ++ @produce_events ++ @fetch_events ++ @consumer_events
  end

  @doc "Returns request-related telemetry events."
  @spec request_events() :: [list(atom())]
  def request_events, do: @request_events

  @doc "Returns connection-related telemetry events."
  @spec connection_events() :: [list(atom())]
  def connection_events, do: @connection_events

  @doc "Returns produce-related telemetry events."
  @spec produce_events() :: [list(atom())]
  def produce_events, do: @produce_events

  @doc "Returns fetch-related telemetry events."
  @spec fetch_events() :: [list(atom())]
  def fetch_events, do: @fetch_events

  @doc "Returns consumer-related telemetry events."
  @spec consumer_events() :: [list(atom())]
  def consumer_events, do: @consumer_events

  # ---------------------------------------------------------------------------
  # Helper functions for emitting events
  # ---------------------------------------------------------------------------

  @doc false
  @spec span(list(atom()), map(), (-> {result, map()})) :: result when result: any()
  def span(event_prefix, metadata, fun) do
    :telemetry.span(event_prefix, metadata, fn ->
      {result, stop_metadata} = fun.()
      stop_metadata_with_result = add_result_to_metadata(result, stop_metadata)
      {result, stop_metadata_with_result}
    end)
  end

  defp add_result_to_metadata({{:ok, _}, _state}, metadata), do: Map.put(metadata, :result, :ok)
  defp add_result_to_metadata({:ok, _}, metadata), do: Map.put(metadata, :result, :ok)

  defp add_result_to_metadata({{:error, error}, _state}, metadata),
    do: Map.merge(metadata, %{result: :error, error: error})

  defp add_result_to_metadata({:error, error}, metadata), do: Map.merge(metadata, %{result: :error, error: error})
  defp add_result_to_metadata(_other, metadata), do: metadata

  @doc false
  @spec request_metadata(struct(), map()) :: map()
  def request_metadata(request, broker_info) do
    {operation, api_version} = @protocol.request_info(request)

    %{
      operation: operation,
      api_version: api_version,
      correlation_id: Map.get(request, :correlation_id),
      client_id: Map.get(request, :client_id),
      broker: broker_info
    }
  end

  @doc false
  @spec connection_metadata(binary() | charlist(), integer(), boolean()) :: map()
  def connection_metadata(host, port, ssl) do
    %{host: to_string(host), port: port, ssl: ssl}
  end

  @doc false
  @spec produce_metadata(binary(), integer(), binary(), integer()) :: map()
  def produce_metadata(topic, partition, client_id, required_acks) do
    %{
      topic: topic,
      partition: partition,
      client_id: client_id,
      required_acks: required_acks
    }
  end

  @doc false
  @spec fetch_metadata(binary(), integer(), integer(), binary()) :: map()
  def fetch_metadata(topic, partition, offset, client_id) do
    %{
      topic: topic,
      partition: partition,
      offset: offset,
      client_id: client_id
    }
  end

  @doc false
  @spec commit_metadata(binary(), binary(), binary(), integer()) :: map()
  def commit_metadata(group_id, client_id, topic, partition_count) do
    %{
      group_id: group_id,
      client_id: client_id,
      topic: topic,
      partition_count: partition_count
    }
  end
end
