defmodule KafkaEx.New.Client do
  @moduledoc """
  Kayrock-compatible KafkaEx.Server implementation

  This implementation attempts to keep as much Kafka 'business logic' as possible
  out of the server implementation, with the motivation that this should make
  the client easier to maintain as the Kafka protocol evolves.

  This implementation does, however, include implementations of all of the
  legacy KafkaEx.Server `handle_call` clauses so that it can be compatible with
  the legacy KafkaEx API.
  """

  alias KafkaEx.Config
  alias KafkaEx.NetworkClient

  alias KafkaEx.New.Client.RequestBuilder
  alias KafkaEx.New.Client.ResponseParser
  alias KafkaEx.New.Structs.Broker
  alias KafkaEx.New.Structs.ClusterMetadata
  alias KafkaEx.New.Structs.NodeSelector

  alias KafkaEx.New.Client.State

  use GenServer

  @type args :: [
          KafkaEx.worker_setting()
          | {:allow_auto_topic_creation, boolean}
        ]

  @doc """
  Start the server in a supervision tree
  """
  @spec start_link(args, atom) :: GenServer.on_start()
  def start_link(args, name \\ __MODULE__)

  def start_link(args, :no_name) do
    GenServer.start_link(__MODULE__, [args, nil])
  end

  def start_link(args, name) do
    GenServer.start_link(__MODULE__, [args, name], name: name)
  end

  @doc """
  Send a Kayrock request to the appropriate broker

  Broker metadata will be updated if necessary
  """
  @spec send_request(
          KafkaEx.New.KafkaExAPI.client(),
          map,
          KafkaEx.New.Structs.NodeSelector.t(),
          pos_integer | nil
        ) :: {:ok, term} | {:error, term}
  def send_request(
        server,
        request,
        node_selector,
        timeout \\ nil
      ) do
    GenServer.call(
      server,
      {:kayrock_request, request, node_selector},
      timeout_val(timeout)
    )
  end

  require Logger
  alias KafkaEx.NetworkClient

  # Default from GenServer
  @default_call_timeout 5_000
  @retry_count 3
  @sync_timeout 1_000

  @impl true
  def init([args, name]) do
    state = State.static_init(args, name || self())

    unless KafkaEx.valid_consumer_group?(state.consumer_group_for_auto_commit) do
      raise KafkaEx.InvalidConsumerGroupError,
            state.consumer_group_for_auto_commit
    end

    brokers =
      state.bootstrap_uris
      |> Enum.with_index()
      |> Enum.into(%{}, fn {{host, port}, ix} ->
        {ix + 1,
         %Broker{
           host: host,
           port: port,
           socket:
             NetworkClient.create_socket(
               host,
               port,
               state.ssl_options,
               state.use_ssl
             )
         }}
      end)

    check_brokers_sockets!(brokers)

    state = %{state | cluster_metadata: %ClusterMetadata{brokers: brokers}}

    {ok_or_err, api_versions, state} = get_api_versions(state)

    if ok_or_err == :error do
      sleep_for_reconnect()
      raise "Brokers sockets are closed"
    end

    :no_error = Kayrock.ErrorCode.code_to_atom(api_versions.error_code)

    initial_topics = Keyword.get(args, :initial_topics, [])

    state = State.ingest_api_versions(state, api_versions)

    state =
      try do
        update_metadata(state, initial_topics)
      rescue
        e ->
          sleep_for_reconnect()
          Kernel.reraise(e, __STACKTRACE__)
      end

    {:ok, _} = :timer.send_interval(state.metadata_update_interval, :update_metadata)

    {:ok, state}
  end

  @impl true
  def handle_call(:cluster_metadata, _from, state) do
    {:reply, {:ok, state.cluster_metadata}, state}
  end

  def handle_call(:correlation_id, _from, state) do
    {:reply, {:ok, state.correlation_id}, state}
  end

  def handle_call(:update_metadata, _from, state) do
    updated_state = update_metadata(state)
    {:reply, {:ok, updated_state.cluster_metadata}, updated_state}
  end

  def handle_call(
        {:set_consumer_group_for_auto_commit, consumer_group},
        _from,
        state
      ) do
    if KafkaEx.valid_consumer_group?(consumer_group) do
      {:reply, :ok, %{state | consumer_group_for_auto_commit: consumer_group}}
    else
      {:reply, {:error, :invalid_consumer_group}, state}
    end
  end

  def handle_call({:topic_metadata, topics, allow_topic_creation}, _from, state) do
    {topic_metadata, updated_state} = fetch_topics_metadata(state, topics, allow_topic_creation)

    {:reply, {:ok, topic_metadata}, updated_state}
  end

  def handle_call({:describe_groups, [consumer_group_name]}, _from, state) do
    if KafkaEx.valid_consumer_group?(consumer_group_name) do
      {response, updated_state} = describe_group_request(consumer_group_name, state)

      {:reply, response, updated_state}
    else
      {:reply, {:error, :invalid_consumer_group}, state}
    end
  end

  def handle_call({:kayrock_request, request, node_selector}, _from, state) do
    {response, updated_state} = kayrock_network_request(request, node_selector, state)

    {:reply, response, updated_state}
  end

  # injects backwards-compatible handle_call clauses
  use KafkaEx.New.ClientCompatibility

  @impl true
  def handle_info(:update_metadata, state) do
    {:noreply, update_metadata(state)}
  end

  def handle_info({:tcp_closed, socket}, state) do
    state_out = close_broker_by_socket(state, socket)
    {:noreply, state_out}
  end

  def handle_info({:ssl_closed, socket}, state) do
    state_out = close_broker_by_socket(state, socket)
    {:noreply, state_out}
  end

  @impl true
  def terminate(reason, state) do
    Logger.log(
      :debug,
      "Shutting down worker #{inspect(state.worker_name)}, " <>
        "reason: #{inspect(reason)}"
    )

    Enum.each(State.brokers(state), fn broker ->
      NetworkClient.close_socket(broker.socket)
    end)
  end

  defp update_metadata(state, topics \\ []) do
    # make sure we update metadata about known topics
    known_topics = ClusterMetadata.known_topics(state.cluster_metadata)
    topics = Enum.uniq(known_topics ++ topics)

    {updated_state, response} =
      retrieve_metadata(
        state,
        config_sync_timeout(),
        topics
      )

    case response do
      nil ->
        updated_state

      _ ->
        new_cluster_metadata = ClusterMetadata.from_metadata_v1_response(response)

        {updated_cluster_metadata, brokers_to_close} =
          ClusterMetadata.merge_brokers(
            updated_state.cluster_metadata,
            new_cluster_metadata
          )

        for broker <- brokers_to_close do
          Logger.log(
            :debug,
            "Closing connection to broker #{broker.node_id}: " <>
              "#{inspect(broker.host)} on port #{inspect(broker.port)}"
          )

          NetworkClient.close_socket(broker.socket)
        end

        updated_state =
          State.update_brokers(
            %{updated_state | cluster_metadata: updated_cluster_metadata},
            &maybe_connect_broker(&1, state)
          )

        updated_state
    end
  end

  defp describe_group_request(consumer_group_name, state) do
    node_selector = NodeSelector.consumer_group(consumer_group_name)

    [consumer_group_name]
    |> RequestBuilder.describe_groups_request(state)
    |> handle_describe_group_request(node_selector, state)
  end

  defp handle_describe_group_request(
         _,
         _,
         _,
         retry_count \\ @retry_count,
         _last_error \\ nil
       )

  defp handle_describe_group_request(_, _, state, 0, last_error) do
    {{:error, last_error}, state}
  end

  defp handle_describe_group_request(
         request,
         node_selector,
         state,
         retry_count,
         _last_error
       ) do
    case kayrock_network_request(request, node_selector, state) do
      {{:ok, response}, state_out} ->
        case ResponseParser.describe_groups_response(response) do
          {:ok, consumer_groups} ->
            {{:ok, consumer_groups}, state_out}

          {:error, [error | _]} ->
            Logger.warn(
              "Unable to fetch consumer group metadata for #{inspect(request.group_ids)}"
            )

            handle_describe_group_request(
              request,
              node_selector,
              state,
              retry_count - 1,
              error
            )
        end

      {_, _state_out} ->
        Logger.warn("Unable to fetch consumer group metadata for #{inspect(request.group_ids)}")

        handle_describe_group_request(
          request,
          node_selector,
          state,
          retry_count - 1,
          :unknown
        )
    end
  end

  defp maybe_connect_broker(broker, state) do
    case Broker.connected?(broker) do
      true ->
        broker

      false ->
        %{
          broker
          | socket:
              NetworkClient.create_socket(
                broker.host,
                broker.port,
                state.ssl_options,
                state.use_ssl
              )
        }
    end
  end

  defp retrieve_metadata(
         state,
         sync_timeout,
         topics
       ) do
    retrieve_metadata(
      state,
      sync_timeout,
      topics,
      @retry_count,
      0
    )
  end

  defp retrieve_metadata(
         state,
         _sync_timeout,
         topics,
         0,
         error_code
       ) do
    Logger.log(
      :error,
      "Metadata request for topics #{inspect(topics)} failed " <>
        "with error_code #{inspect(error_code)}"
    )

    {state, nil}
  end

  defp retrieve_metadata(
         state,
         sync_timeout,
         topics,
         retry,
         _error_code
       ) do
    # default to version 4 of the metadata protocol because this one treats an
    # empty list of topics as 'no topics'.  note this limits us to Kafka 0.11+
    api_version = State.max_supported_api_version(state, :metadata, 4)

    metadata_request = %{
      Kayrock.Metadata.get_request_struct(api_version)
      | topics: topics,
        allow_auto_topic_creation: state.allow_auto_topic_creation
    }

    {{ok_or_err, response}, state_out} =
      kayrock_network_request(
        metadata_request,
        NodeSelector.first_available(),
        state
      )

    case ok_or_err do
      :ok ->
        case Enum.find(
               response.topic_metadata,
               &(&1.error_code ==
                   Kayrock.ErrorCode.atom_to_code!(:leader_not_available))
             ) do
          nil ->
            {state_out, response}

          topic_metadata ->
            :timer.sleep(300)

            retrieve_metadata(
              state,
              sync_timeout,
              topics,
              retry - 1,
              topic_metadata.error_code
            )
        end

      _ ->
        message = "Unable to fetch metadata from any brokers. Timeout is #{sync_timeout}."

        Logger.log(:error, message)
        raise message
        {state_out, nil}
    end
  end

  defp sleep_for_reconnect() do
    Process.sleep(Application.get_env(:kafka_ex, :sleep_for_reconnect, 400))
  end

  defp check_brokers_sockets!(brokers) do
    any_socket_opened =
      brokers
      |> Enum.map(fn {_, %Broker{socket: socket}} -> !is_nil(socket) end)
      |> Enum.reduce(&(&1 || &2))

    if !any_socket_opened do
      sleep_for_reconnect()
      raise "Brokers sockets are not opened"
    end
  end

  defp client_request(request, state) do
    %{
      request
      | client_id: Config.client_id(),
        correlation_id: state.correlation_id
    }
  end

  # select a broker, updating state if necessary (e.g., metadata or consumer group)
  # returns {broker, maybe_updated_state} - broker will be nil in case of
  # failure
  defp select_broker_with_update(state, selector, state_updater) do
    case State.select_broker(state, selector) do
      {:error, _} ->
        updated_state = state_updater.(state)

        case State.select_broker(updated_state, selector) do
          {:error, _} ->
            {nil, updated_state}

          {:ok, broker} ->
            {broker, updated_state}
        end

      {:ok, broker} ->
        {broker, state}
    end
  end

  defp broker_for_partition_with_update(state, topic, partition) do
    select_broker_with_update(
      state,
      NodeSelector.topic_partition(topic, partition),
      &update_metadata(&1, [topic])
    )
  end

  defp broker_for_consumer_group_with_update(state, consumer_group) do
    select_broker_with_update(
      state,
      NodeSelector.consumer_group(consumer_group),
      &update_consumer_group_coordinator(&1, consumer_group)
    )
  end

  defp update_consumer_group_coordinator(state, consumer_group) do
    request = %Kayrock.FindCoordinator.V1.Request{
      coordinator_key: consumer_group,
      coordinator_type: 0
    }

    {response, updated_state} =
      kayrock_network_request(request, NodeSelector.first_available(), state)

    case response do
      {:ok,
       %Kayrock.FindCoordinator.V1.Response{
         error_code: 0,
         coordinator: coordinator
       }} ->
        State.put_consumer_group_coordinator(
          updated_state,
          consumer_group,
          coordinator.node_id
        )

      {:ok,
       %Kayrock.FindCoordinator.V1.Response{
         error_code: error_code
       }} ->
        Logger.warning(
          "Unable to find consumer group coordinator for " <>
            "#{inspect(consumer_group)}: Error " <>
            "#{Kayrock.ErrorCode.code_to_atom(error_code)}"
        )

        updated_state
    end
  end

  defp first_broker_response(request, brokers, timeout) do
    brokers
    |> Enum.shuffle()
    |> Enum.find_value(brokers, fn broker ->
      if Broker.connected?(broker) do
        try_broker(broker, request, timeout)
      end
    end)
  end

  defp try_broker(broker, request, timeout) do
    case NetworkClient.send_sync_request(broker, request, timeout) do
      {:error, error} ->
        Logger.warning("Network call resulted in error: #{inspect(error)}")
        nil

      response ->
        response
    end
  end

  defp timeout_val(nil) do
    Application.get_env(:kafka_ex, :sync_timeout, @default_call_timeout)
  end

  defp timeout_val(timeout) when is_integer(timeout), do: timeout

  defp config_sync_timeout(timeout \\ nil) do
    timeout || Application.get_env(:kafka_ex, :sync_timeout, @sync_timeout)
  end

  defp default_partitioner do
    Application.get_env(:kafka_ex, :partitioner, KafkaEx.DefaultPartitioner)
  end

  defp consumer_group_if_auto_commit?(true, state), do: consumer_group?(state)
  defp consumer_group_if_auto_commit?(false, _state), do: true

  # note within the GenServer state, we've already validated the
  # consumer group, so it can only be either :no_consumer_group or a
  # valid binary consumer group name
  defp consumer_group?(%State{
         consumer_group_for_auto_commit: :no_consumer_group
       }) do
    false
  end

  defp consumer_group?(_), do: true

  defp get_api_versions(state, request_version \\ 0) do
    request = Kayrock.ApiVersions.get_request_struct(request_version)

    {{ok_or_error, response}, state_out} =
      kayrock_network_request(request, NodeSelector.first_available(), state)

    {ok_or_error, response, state_out}
  end

  defp kayrock_network_request(
         request,
         node_selector,
         state,
         network_timeout \\ nil
       ) do
    # produce request have an acks field and if this is 0 then we do not want to
    # wait for a response from the broker
    synchronous =
      case Map.get(request, :acks) do
        0 -> false
        _ -> true
      end

    network_timeout = config_sync_timeout(network_timeout)

    {send_request, updated_state} =
      get_send_request_function(
        node_selector,
        state,
        network_timeout,
        synchronous
      )

    case send_request do
      :no_broker ->
        {{:error, :no_broker}, updated_state}

      _ ->
        response =
          run_client_request(
            client_request(request, updated_state),
            send_request,
            synchronous
          )

        {response, State.increment_correlation_id(updated_state)}
    end
  end

  defp run_client_request(
         %{client_id: client_id, correlation_id: correlation_id} = client_request,
         send_request,
         synchronous
       )
       when not is_nil(client_id) and not is_nil(correlation_id) do
    wire_request = Kayrock.Request.serialize(client_request)

    case(send_request.(wire_request)) do
      {:error, reason} ->
        {:error, reason}

      data ->
        if synchronous do
          {:ok, deserialize(data, client_request)}
        else
          data
        end
    end
  end

  defp get_send_request_function(
         %NodeSelector{strategy: :first_available},
         state,
         network_timeout,
         _synchronous
       ) do
    {fn wire_request ->
       first_broker_response(
         wire_request,
         State.brokers(state),
         network_timeout
       )
     end, state}
  end

  defp get_send_request_function(
         %NodeSelector{
           strategy: :topic_partition,
           topic: topic,
           partition: partition
         },
         state,
         network_timeout,
         synchronous
       ) do
    {broker, updated_state} =
      broker_for_partition_with_update(
        state,
        topic,
        partition
      )

    if broker do
      if synchronous do
        {fn wire_request ->
           NetworkClient.send_sync_request(
             broker,
             wire_request,
             network_timeout
           )
         end, updated_state}
      else
        {fn wire_request ->
           NetworkClient.send_async_request(broker, wire_request)
         end, updated_state}
      end
    else
      {:no_broker, updated_state}
    end
  end

  defp get_send_request_function(
         %NodeSelector{
           strategy: :consumer_group,
           consumer_group_name: consumer_group
         },
         state,
         network_timeout,
         _synchronous
       ) do
    {broker, updated_state} =
      broker_for_consumer_group_with_update(
        state,
        consumer_group
      )

    {fn wire_request ->
       NetworkClient.send_sync_request(
         broker,
         wire_request,
         network_timeout
       )
     end, updated_state}
  end

  defp get_send_request_function(
         %NodeSelector{} = node_selector,
         state,
         network_timeout,
         _synchronous
       ) do
    {:ok, broker} = State.select_broker(state, node_selector)

    {fn wire_request ->
       NetworkClient.send_sync_request(
         broker,
         wire_request,
         network_timeout
       )
     end, state}
  end

  defp deserialize(data, request) do
    try do
      deserializer = Kayrock.Request.response_deserializer(request)
      {resp, _} = deserializer.(data)
      resp
    rescue
      _ ->
        Logger.error(
          "Failed to parse a response from the server: " <>
            inspect(data, limit: :infinity) <>
            " for request #{inspect(request, limit: :infinity)}"
        )

        Kernel.reraise(
          "Parse error during #{inspect(request)} response deserializer. " <>
            "Couldn't parse: #{inspect(data)}",
          __STACKTRACE__
        )
    end
  end

  defp fetch_topics_metadata(state, topics, allow_topic_creation) do
    allow_auto_topic_creation = state.allow_auto_topic_creation

    updated_state =
      update_metadata(
        %{state | allow_auto_topic_creation: allow_topic_creation},
        topics
      )

    topic_metadata = State.topics_metadata(updated_state, topics)

    {topic_metadata, %{updated_state | allow_auto_topic_creation: allow_auto_topic_creation}}
  end

  defp close_broker_by_socket(state, socket) do
    State.update_brokers(state, fn broker ->
      if Broker.has_socket?(broker, socket) do
        Logger.log(
          :debug,
          "Broker #{inspect(broker.host)}:#{inspect(broker.port)} closed connection"
        )

        Broker.put_socket(broker, nil)
      else
        broker
      end
    end)
  end
end
