defmodule KafkaEx.ServerKayrock do
  @moduledoc """
  Defines the KafkaEx.Server behavior that all Kafka API servers must implement, this module also provides some common callback functions that are injected into the servers that `use` it.
  """

  alias KafkaEx.NetworkClient
  alias KafkaEx.Protocol.Metadata
  alias KafkaEx.Protocol.Metadata.Response, as: MetadataResponse
  alias KafkaEx.Protocol.Produce
  alias KafkaEx.Socket

  alias KafkaEx.New.Adapter
  alias KafkaEx.New.ApiVersions
  alias KafkaEx.New.Broker
  alias KafkaEx.New.ClusterMetadata

  defmodule State do
    @moduledoc false

    alias KafkaEx.New.ClusterMetadata
    alias KafkaEx.New.ConsumerGroupMetadata

    defstruct(
      cluster_metadata: %ClusterMetadata{},
      event_pid: nil,
      consumer_metadata: %ConsumerGroupMetadata{},
      correlation_id: 0,
      consumer_group: nil,
      metadata_update_interval: nil,
      consumer_group_update_interval: nil,
      worker_name: KafkaEx.Server,
      ssl_options: [],
      use_ssl: false,
      api_versions: %{}
    )

    @type t :: %__MODULE__{}

    @spec increment_correlation_id(t) :: t
    def increment_correlation_id(%State{correlation_id: cid} = state) do
      %{state | correlation_id: cid + 1}
    end

    require Logger

    def select_broker(
          %State{cluster_metadata: cluster_metadata},
          selector
        ) do
      with {:ok, node_id} <-
             ClusterMetadata.select_node(cluster_metadata, selector),
           broker <-
             ClusterMetadata.broker_by_node_id(cluster_metadata, node_id) do
        {:ok, broker}
      else
        err -> err
      end
    end

    def update_brokers(%State{cluster_metadata: cluster_metadata} = state, cb)
        when is_function(cb, 1) do
      %{
        state
        | cluster_metadata: ClusterMetadata.update_brokers(cluster_metadata, cb)
      }
    end
  end

  use GenServer

  # Default from GenServer
  @default_call_timeout 5_000

  def start_link(args, name \\ __MODULE__)

  def start_link(args, :no_name) do
    GenServer.start_link(__MODULE__, [args])
  end

  def start_link(args, name) do
    GenServer.start_link(__MODULE__, [args, name], name: name)
  end

  def kayrock_call(server, request, node_selector, opts \\ []) do
    call(server, {:kayrock_request, request, node_selector}, opts)
  end

  @doc false
  @spec call(
          GenServer.server(),
          atom | tuple,
          nil | number | (opts :: Keyword.t())
        ) :: term
  def call(server, request, opts \\ [])

  def call(server, request, opts) when is_list(opts) do
    call(server, request, opts[:timeout])
  end

  def call(server, request, nil) do
    # If using the configured sync_timeout that is less than the default
    # GenServer.call timeout, use the larger value unless explicitly set
    # using opts[:timeout].
    timeout =
      max(
        @default_call_timeout,
        Application.get_env(:kafka_ex, :sync_timeout, @default_call_timeout)
      )

    call(server, request, timeout)
  end

  def call(server, request, timeout) when is_integer(timeout) do
    GenServer.call(server, request, timeout)
  end

  # credo:disable-for-next-line Credo.Check.Refactor.LongQuoteBlocks
  require Logger
  alias KafkaEx.NetworkClient
  alias KafkaEx.Protocol.Offset

  @client_id "kafka_ex"
  @retry_count 3
  @wait_time 10
  @min_bytes 1
  @max_bytes 1_000_000
  @metadata_update_interval 30_000
  @sync_timeout 1_000
  @ssl_options []

  def init([args]) do
    init([args, self()])
  end

  def init([args, name]) do
    uris = Keyword.get(args, :uris, [])

    metadata_update_interval =
      Keyword.get(args, :metadata_update_interval, @metadata_update_interval)

    use_ssl = Keyword.get(args, :use_ssl, false)
    ssl_options = Keyword.get(args, :ssl_options, [])

    brokers =
      Enum.into(Enum.with_index(uris), %{}, fn {{host, port}, ix} ->
        {ix + 1,
         %Broker{
           host: host,
           port: port,
           socket: NetworkClient.create_socket(host, port, ssl_options, use_ssl)
         }}
      end)

    check_brokers_sockets!(brokers)

    # in kayrock we manage the consumer group elsewhere
    consumer_group = :no_consumer_group

    state = %State{
      consumer_group: consumer_group,
      metadata_update_interval: metadata_update_interval,
      consumer_group_update_interval: nil,
      worker_name: name,
      ssl_options: ssl_options,
      use_ssl: use_ssl,
      api_versions: %{},
      cluster_metadata: %ClusterMetadata{brokers: brokers}
    }

    {ok_or_err, api_versions, state} = get_api_versions(state)

    if ok_or_err == :error do
      sleep_for_reconnect()
      raise "Brokers sockets are closed"
    end

    :no_error = Kayrock.ErrorCode.code_to_atom(api_versions.error_code)

    api_versions = ApiVersions.from_response(api_versions)
    state = %{state | api_versions: api_versions}

    state =
      try do
        update_metadata(state)
      rescue
        e ->
          sleep_for_reconnect()
          Kernel.reraise(e, System.stacktrace())
      end

    state = %{
      state
      | consumer_group: consumer_group,
        metadata_update_interval: metadata_update_interval,
        consumer_group_update_interval: nil,
        worker_name: name,
        ssl_options: ssl_options,
        use_ssl: use_ssl,
        api_versions: api_versions
    }

    # Get the initial "real" broker list and start a regular refresh cycle.
    state = update_metadata(state)

    {:ok, _} =
      :timer.send_interval(state.metadata_update_interval, :update_metadata)

    {:ok, state}
  end

  def handle_call(:update_metadata, _from, state) do
    updated_state = update_metadata(state)
    {:reply, {:ok, updated_state.cluster_metadata}, updated_state}
  end

  def handle_call({:topic_metadata, topics}, _from, state) do
    updated_state = update_metadata(state, topics)
    # todo should live in clustermetadata
    topic_metadata =
      updated_state.cluster_metadata.topics
      |> Map.take(topics)
      |> Map.values()

    {:reply, {:ok, topic_metadata}, updated_state}
  end

  def handle_call({:offset, topic, partition, time}, _from, state) do
    request = Adapter.list_offsets_request(topic, partition, time)

    {response, updated_state} =
      kayrock_network_request(
        request,
        {:topic_partition, topic, partition},
        state
      )

    adapted_response =
      case response do
        {:ok, api_response} ->
          {:ok, Adapter.list_offsets_response(api_response)}

        other ->
          other
      end

    {:reply, adapted_response, updated_state}
  end

  def handle_call({:produce, produce_request}, _from, state) do
    {response, updated_state} = produce(produce_request, state)
    {:reply, response, updated_state}
  end

  def handle_call({:kayrock_request, request, node_selector}, _from, state) do
    {response, updated_state} =
      kayrock_network_request(request, node_selector, state)

    {:reply, response, updated_state}
  end

  #  def handle_call(:consumer_group, _from, state) do
  #    kafka_server_consumer_group(state)
  #  end
  #
  #
  #  def handle_call({:fetch, fetch_request}, _from, state) do
  #    kafka_server_fetch(fetch_request, state)
  #  end
  #
  #
  #  def handle_call({:offset_fetch, offset_fetch}, _from, state) do
  #    kafka_server_offset_fetch(offset_fetch, state)
  #  end
  #
  #  def handle_call({:offset_commit, offset_commit_request}, _from, state) do
  #    kafka_server_offset_commit(offset_commit_request, state)
  #  end
  #
  #  def handle_call({:consumer_group_metadata, _consumer_group}, _from, state) do
  #    kafka_server_consumer_group_metadata(state)
  #  end
  #
  #  def handle_call({:metadata, topic}, _from, state) do
  #    kafka_server_metadata(topic, state)
  #  end
  #
  #  def handle_call({:join_group, request, network_timeout}, _from, state) do
  #    kafka_server_join_group(request, network_timeout, state)
  #  end
  #
  #  def handle_call({:sync_group, request, network_timeout}, _from, state) do
  #    kafka_server_sync_group(request, network_timeout, state)
  #  end
  #
  #  def handle_call({:leave_group, request, network_timeout}, _from, state) do
  #    kafka_server_leave_group(request, network_timeout, state)
  #  end
  #
  #  def handle_call({:heartbeat, request, network_timeout}, _from, state) do
  #    kafka_server_heartbeat(request, network_timeout, state)
  #  end
  #
  #  def handle_call({:create_topics, requests, network_timeout}, _from, state) do
  #    kafka_server_create_topics(requests, network_timeout, state)
  #  end
  #
  #  def handle_call({:delete_topics, topics, network_timeout}, _from, state) do
  #    kafka_server_delete_topics(topics, network_timeout, state)
  #  end
  #
  #  def handle_call({:api_versions}, _from, state) do
  #    kafka_server_api_versions(state)
  #  end
  #
  def handle_info(:update_metadata, state) do
    {:noreply, update_metadata(state)}
  end

  #
  #  def handle_info(:update_consumer_metadata, state) do
  #    kafka_server_update_consumer_metadata(state)
  #  end
  #
  #  def handle_info(_, state) do
  #    {:noreply, state}
  #  end
  #
  #  def terminate(reason, state) do
  #    Logger.log(
  #      :debug,
  #      "Shutting down worker #{inspect(state.worker_name)}, reason: #{
  #        inspect(reason)
  #      }"
  #    )
  #
  #    if state.event_pid do
  #      :gen_event.stop(state.event_pid)
  #    end
  #
  #    Enum.each(state.brokers, fn broker ->
  #      NetworkClient.close_socket(broker.socket)
  #    end)
  #  end
  #
  #  # KakfaEx.Server behavior default implementations
  #  # This needs a refactor, but for now make credo pass:
  #  # credo:disable-for-next-line Credo.Check.Refactor.CyclomaticComplexity
  #  def kafka_server_produce(
  #        produce_request,
  #        %State{metadata: metadata} = state
  #      ) do
  #    correlation_id = state.correlation_id + 1
  #
  #    produce_request =
  #      default_partitioner().assign_partition(produce_request, metadata)
  #
  #    produce_request_data =
  #      try do
  #        Produce.create_request(correlation_id, @client_id, produce_request)
  #      rescue
  #        e in FunctionClauseError -> nil
  #      end
  #
  #    case produce_request_data do
  #      nil ->
  #        {:reply, {:error, "Invalid produce request"}, state}
  #
  #      _ ->
  #        kafka_server_produce_send_request(
  #          correlation_id,
  #          produce_request,
  #          produce_request_data,
  #          state
  #        )
  #    end
  #  end
  #
  #  # credo:disable-for-next-line Credo.Check.Refactor.CyclomaticComplexity
  #  def kafka_server_produce_send_request(
  #        correlation_id,
  #        produce_request,
  #        produce_request_data,
  #        state
  #      ) do
  #    {broker, state, corr_id} =
  #      case MetadataResponse.broker_for_topic(
  #             state.metadata,
  #             state.brokers,
  #             produce_request.topic,
  #             produce_request.partition
  #           ) do
  #        nil ->
  #          {retrieved_corr_id, _} =
  #            retrieve_metadata(
  #              state.brokers,
  #              state.correlation_id,
  #              config_sync_timeout(),
  #              produce_request.topic,
  #              state.api_versions
  #            )
  #
  #          state = update_metadata(%{state | correlation_id: retrieved_corr_id})
  #
  #          {
  #            MetadataResponse.broker_for_topic(
  #              state.metadata,
  #              state.brokers,
  #              produce_request.topic,
  #              produce_request.partition
  #            ),
  #            state,
  #            retrieved_corr_id
  #          }
  #
  #        broker ->
  #          {broker, state, correlation_id}
  #      end
  #
  #    response =
  #      case broker do
  #        nil ->
  #          Logger.log(
  #            :error,
  #            "kafka_server_produce_send_request: leader for topic #{
  #              produce_request.topic
  #            }/#{produce_request.partition} is not available"
  #          )
  #
  #          :leader_not_available
  #
  #        broker ->
  #          case produce_request.required_acks do
  #            0 ->
  #              NetworkClient.send_async_request(broker, produce_request_data)
  #
  #            _ ->
  #              response =
  #                broker
  #                |> NetworkClient.send_sync_request(
  #                  produce_request_data,
  #                  config_sync_timeout()
  #                )
  #                |> case do
  #                  {:error, reason} -> reason
  #                  response -> Produce.parse_response(response)
  #                end
  #
  #              # credo:disable-for-next-line Credo.Check.Refactor.Nesting
  #              case response do
  #                [
  #                  %KafkaEx.Protocol.Produce.Response{
  #                    partitions: [%{error_code: :no_error, offset: offset}],
  #                    topic: topic
  #                  }
  #                ]
  #                when offset != nil ->
  #                  {:ok, offset}
  #
  #                _ ->
  #                  {:error, response}
  #              end
  #          end
  #      end
  #
  #    state = %{state | correlation_id: corr_id + 1}
  #    {:reply, response, state}
  #  end
  #
  #  def kafka_server_metadata(topic, state) do
  #    {correlation_id, metadata} =
  #      retrieve_metadata(
  #        state.brokers,
  #        state.correlation_id,
  #        config_sync_timeout(),
  #        topic,
  #        state.api_versions
  #      )
  #
  #    updated_state = %{
  #      state
  #      | metadata: metadata,
  #        correlation_id: correlation_id
  #    }
  #
  #    {:reply, metadata, updated_state}
  #  end
  #
  #  def kafka_server_update_metadata(state) do
  #    {:noreply, update_metadata(state)}
  #  end

  def update_metadata(state, topics \\ []) do
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
        Logger.debug("WAS NIL")
        updated_state

      _ ->
        new_cluster_metadata =
          ClusterMetadata.from_metadata_v1_response(response)

        {updated_cluster_metadata, brokers_to_close} =
          ClusterMetadata.merge_brokers(
            updated_state.cluster_metadata,
            new_cluster_metadata
          )

        Logger.debug("BROKERS TO CLOSE: #{inspect(brokers_to_close)}")

        for broker <- brokers_to_close do
          Logger.log(
            :debug,
            "Closing connection to broker #{broker.node_id}: #{
              inspect(broker.host)
            } on port #{inspect(broker.port)}"
          )

          NetworkClient.close_socket(broker.socket)
        end

        updated_state =
          State.update_brokers(
            %{updated_state | cluster_metadata: updated_cluster_metadata},
            fn b ->
              case Broker.connected?(b) do
                true ->
                  b

                false ->
                  %{
                    b
                    | socket:
                        NetworkClient.create_socket(
                          b.host,
                          b.port,
                          state.ssl_options,
                          state.use_ssl
                        )
                  }
              end
            end
          )

        Logger.debug("UPDATED METADATA #{inspect(updated_state)}")

        updated_state
    end

    ## this will probably also not work
    # metadata_brokers =
    #  metadata.brokers
    #  |> Enum.map(&%{&1 | is_controller: &1.node_id == metadata.controller_id})

    # brokers =
    #  state.brokers
    #  |> remove_stale_brokers(metadata_brokers)
    #  |> add_new_brokers(metadata_brokers, state.ssl_options, state.use_ssl)

    # %{
    #  state
    #  | metadata: metadata,
    #    brokers: brokers,
    #    correlation_id: correlation_id + 1
    # }
  end

  # credo:disable-for-next-line Credo.Check.Refactor.FunctionArity
  def retrieve_metadata(
        state,
        sync_timeout,
        topics \\ []
      ) do
    retrieve_metadata(
      state,
      sync_timeout,
      topics,
      @retry_count,
      0
    )
  end

  # credo:disable-for-next-line Credo.Check.Refactor.FunctionArity
  def retrieve_metadata(
        state,
        sync_timeout,
        topics,
        retry,
        error_code
      ) do
    # default to version 4 of the metdata protocol because this one treats an
    # empty list of topics as 'no topics'.  note this limits us to kafka 0.11+
    api_version =
      ApiVersions.max_supported_version(state.api_versions, :metadata, 4)

    retrieve_metadata_with_version(
      state,
      sync_timeout,
      topics,
      retry,
      error_code,
      api_version
    )
  end

  # credo:disable-for-next-line Credo.Check.Refactor.FunctionArity
  def retrieve_metadata_with_version(
        state,
        _sync_timeout,
        topics,
        0,
        error_code,
        _api_version
      ) do
    Logger.log(
      :error,
      "Metadata request for topics #{inspect(topics)} failed " <>
        "with error_code #{inspect(error_code)}"
    )

    {state, nil}
  end

  # credo:disable-for-next-line Credo.Check.Refactor.FunctionArity
  def retrieve_metadata_with_version(
        state,
        sync_timeout,
        topics,
        retry,
        _error_code,
        api_version
      ) do
    metadata_request = %{
      Kayrock.Metadata.get_request_struct(api_version)
      | topics: topics
    }

    {{ok_or_err, response}, state_out} =
      kayrock_network_request(metadata_request, :any, state)

    Logger.debug("RETRIEVE METADATA #{inspect(ok_or_err)} #{inspect(response)}")

    case ok_or_err do
      :ok ->
        # THIS WILL PROBABLY NOT WORK
        case Enum.find(
               response.topic_metadata,
               &(&1.error_code ==
                   Kayrock.ErrorCode.atom_to_code!(:leader_not_available))
             ) do
          nil ->
            # HERE update state
            {state_out, response}

          topic_metadata ->
            :timer.sleep(300)

            retrieve_metadata_with_version(
              state,
              sync_timeout,
              topics,
              retry - 1,
              topic_metadata.error_code,
              api_version
            )
        end

      _ ->
        message =
          "Unable to fetch metadata from any brokers. Timeout is #{sync_timeout}."

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

  defp connect_broker(host, port, ssl_opts, use_ssl) do
    %Broker{
      host: host,
      port: port,
      socket: NetworkClient.create_socket(host, port, ssl_opts, use_ssl)
    }
  end

  defp client_request(request, state) do
    %{
      request
      | client_id: @client_id,
        correlation_id: state.correlation_id
    }
  end

  # gets the broker for a given partition, updating metadata if necessary
  # returns {broker, maybe_updated_state}
  defp broker_for_partition_with_update(state, topic, partition) do
    case State.select_broker(state, {:topic_partition, topic, partition}) do
      {:error, _} ->
        updated_state = update_metadata(state, [topic])

        case State.select_broker(
               updated_state,
               {:topic_partition, topic, partition}
             ) do
          {:error, _} ->
            {nil, updated_state}

          {:ok, broker} ->
            {broker, updated_state}
        end

      {:ok, broker} ->
        {broker, state}
    end
  end

  defp first_broker_response(request, brokers, timeout) do
    Enum.find_value(brokers, fn {_node_id, broker} ->
      if Broker.connected?(broker) do
        # credo:disable-for-next-line Credo.Check.Refactor.Nesting
        Logger.debug("SENDING TO #{inspect(broker)}")

        case NetworkClient.send_sync_request(broker, request, timeout) do
          {:error, error} ->
            Logger.debug("GOT ERROR #{inspect(error)}")
            nil

          response ->
            Logger.debug("GOT RESPONSE #{inspect(response)}")
            response
        end
      end
    end)
  end

  defp config_sync_timeout(timeout \\ nil) do
    timeout || Application.get_env(:kafka_ex, :sync_timeout, @sync_timeout)
  end

  defp default_partitioner do
    Application.get_env(:kafka_ex, :partitioner, KafkaEx.DefaultPartitioner)
  end

  ##### NEW CODE

  defp get_api_versions(state, request_version \\ 0) do
    request = Kayrock.ApiVersions.get_request_struct(request_version)

    {{ok_or_error, response}, state_out} =
      kayrock_network_request(request, :any, state)

    {ok_or_error, response, state_out}
  end

  defp kayrock_network_request(request, node_selector, state) do
    {sender, updated_state} = get_sender(node_selector, state)

    Logger.debug(inspect(request))
    Logger.debug(inspect(updated_state))

    wire_request =
      request
      |> client_request(updated_state)
      |> Kayrock.Request.serialize()

    response =
      case(sender.(wire_request)) do
        {:error, reason} -> {:error, reason}
        data -> {:ok, deserialize(data, request)}
      end

    state_out = %{updated_state | correlation_id: state.correlation_id + 1}
    {response, state_out}
  end

  defp get_sender(:any, state) do
    {fn wire_request ->
       first_broker_response(
         wire_request,
         state.cluster_metadata.brokers,
         config_sync_timeout()
       )
     end, state}
  end

  defp get_sender({:topic_partition, topic, partition}, state) do
    Logger.debug("SELECT BROKER #{inspect(state)}")
    # TODO can be cleaned up with select_broker
    {broker, updated_state} =
      broker_for_partition_with_update(
        state,
        topic,
        partition
      )

    Logger.debug("SHOULD SEND TO BROKER #{inspect(broker)}")

    {fn wire_request ->
       NetworkClient.send_sync_request(
         broker,
         wire_request,
         config_sync_timeout()
       )
     end, updated_state}
  end

  defp deserialize(data, request) do
    try do
      deserializer = Kayrock.Request.response_deserializer(request)
      {resp, _} = deserializer.(data)
      resp
    rescue
      _ ->
        Logger.error(
          "Failed to parse a response from the server: #{inspect(data)} " <>
            "for request #{inspect(request)}"
        )

        Kernel.reraise(
          "Parse error during #{inspect(request)} response deserializer. " <>
            "Couldn't parse: #{inspect(data)}",
          System.stacktrace()
        )
    end
  end

  defp produce(request, state) do
    [topic_data | _] = request.topic_data
    [%{partition: partition} | _] = topic_data.data

    kayrock_network_request(
      request,
      {:topic_partition, topic_data.topic, partition},
      state
    )
  end
end
