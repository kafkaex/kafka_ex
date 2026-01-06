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
  """

  @protocol Application.compile_env(:kafka_ex, :protocol, KafkaEx.Protocol.KayrockProtocol)

  @request_start [:kafka_ex, :request, :start]
  @request_stop [:kafka_ex, :request, :stop]
  @request_exception [:kafka_ex, :request, :exception]

  @connection_start [:kafka_ex, :connection, :start]
  @connection_stop [:kafka_ex, :connection, :stop]
  @connection_exception [:kafka_ex, :connection, :exception]

  @request_events [@request_start, @request_stop, @request_exception]
  @connection_events [@connection_start, @connection_stop, @connection_exception]

  @doc """
  Returns the list of all telemetry events emitted by KafkaEx.
  """
  @spec events() :: [list(atom())]
  def events do
    @request_events ++ @connection_events
  end

  @doc "Returns request-related telemetry events."
  @spec request_events() :: [list(atom())]
  def request_events, do: @request_events

  @doc "Returns connection-related telemetry events."
  @spec connection_events() :: [list(atom())]
  def connection_events, do: @connection_events

  # ---------------------------------------------------------------------------
  # Helper functions for emitting events
  # ---------------------------------------------------------------------------

  @doc false
  @spec span(list(atom()), map(), (-> {result, map()})) :: result when result: any()
  def span(event_prefix, metadata, fun) do
    :telemetry.span(event_prefix, metadata, fun)
  end

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
end
