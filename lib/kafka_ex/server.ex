defmodule KafkaEx.Server do
  @moduledoc """
  Defines the KafkaEx.Server behavior that all Kafka API servers must implement, this module also provides some common callback functions that are injected into the servers that `use` it.
  """

  alias KafkaEx.Config
  alias KafkaEx.NetworkClient
  alias KafkaEx.Protocol.ConsumerMetadata
  alias KafkaEx.Protocol.Heartbeat.Request, as: HeartbeatRequest
  alias KafkaEx.Protocol.JoinGroup.Request, as: JoinGroupRequest
  alias KafkaEx.Protocol.LeaveGroup.Request, as: LeaveGroupRequest
  alias KafkaEx.Protocol.Metadata
  alias KafkaEx.Protocol.Metadata.Broker
  alias KafkaEx.Protocol.Metadata.Response, as: MetadataResponse
  alias KafkaEx.Protocol.OffsetCommit.Request, as: OffsetCommitRequest
  alias KafkaEx.Protocol.OffsetFetch.Request, as: OffsetFetchRequest
  alias KafkaEx.Protocol.Fetch.Request, as: FetchRequest
  alias KafkaEx.Protocol.Produce
  alias KafkaEx.Protocol.Produce.Request, as: ProduceRequest
  alias KafkaEx.Protocol.SyncGroup.Request, as: SyncGroupRequest
  alias KafkaEx.Protocol.CreateTopics.TopicRequest, as: CreateTopicsRequest
  alias KafkaEx.Socket

  defmodule State do
    @moduledoc false

    alias KafkaEx.Protocol.Metadata.Response, as: MetadataResponse
    alias KafkaEx.Protocol.Metadata.Broker

    defstruct(
      metadata: %Metadata.Response{},
      brokers: [],
      event_pid: nil,
      consumer_metadata: %ConsumerMetadata.Response{},
      correlation_id: 0,
      consumer_group: nil,
      metadata_update_interval: nil,
      consumer_group_update_interval: nil,
      worker_name: KafkaEx.Server,
      ssl_options: [],
      use_ssl: false,
      api_versions: []
    )

    @type t :: %State{
            metadata: Metadata.Response.t(),
            brokers: [Broker.t()],
            event_pid: nil | pid,
            consumer_metadata: ConsumerMetadata.Response.t(),
            correlation_id: integer,
            metadata_update_interval: nil | integer,
            consumer_group_update_interval: nil | integer,
            worker_name: atom,
            ssl_options: KafkaEx.ssl_options(),
            use_ssl: boolean,
            api_versions: [KafkaEx.Protocol.ApiVersions.ApiVersion]
          }

    @spec increment_correlation_id(t) :: t
    def increment_correlation_id(%State{correlation_id: cid} = state) do
      %{state | correlation_id: cid + 1}
    end

    @spec broker_for_partition(t, binary, integer) :: Broker.t() | nil
    def broker_for_partition(state, topic, partition) do
      MetadataResponse.broker_for_topic(
        state.metadata,
        state.brokers,
        topic,
        partition
      )
    end
  end

  @callback kafka_server_init(args :: [term]) ::
              {:ok, state}
              | {:ok, state, timeout | :hibernate}
              | :ignore
              | {:stop, reason :: any}
            when state: any
  @callback kafka_server_produce(
              request :: ProduceRequest.t(),
              state :: State.t()
            ) ::
              {:reply, reply, new_state}
              | {:reply, reply, new_state, timeout | :hibernate}
              | {:noreply, new_state}
              | {:noreply, new_state, timeout | :hibernate}
              | {:stop, reason, reply, new_state}
              | {:stop, reason, new_state}
            when reply: term, new_state: term, reason: term
  @callback kafka_server_consumer_group(state :: State.t()) ::
              {:reply, reply, new_state}
              | {:reply, reply, new_state, timeout | :hibernate}
              | {:noreply, new_state}
              | {:noreply, new_state, timeout | :hibernate}
              | {:stop, reason, reply, new_state}
              | {:stop, reason, new_state}
            when reply: term, new_state: term, reason: term
  @callback kafka_server_fetch(
              fetch_request :: FetchRequest.t(),
              state :: State.t()
            ) ::
              {:reply, reply, new_state}
              | {:reply, reply, new_state, timeout | :hibernate}
              | {:noreply, new_state}
              | {:noreply, new_state, timeout | :hibernate}
              | {:stop, reason, reply, new_state}
              | {:stop, reason, new_state}
            when reply: term, new_state: term, reason: term
  @callback kafka_server_offset(
              topic :: binary,
              partition :: integer,
              time :: :calendar.datetime() | :latest | :earliest,
              state :: State.t()
            ) ::
              {:reply, reply, new_state}
              | {:reply, reply, new_state, timeout | :hibernate}
              | {:noreply, new_state}
              | {:noreply, new_state, timeout | :hibernate}
              | {:stop, reason, reply, new_state}
              | {:stop, reason, new_state}
            when reply: term, new_state: term, reason: term
  @callback kafka_server_offset_fetch(
              request :: OffsetFetchRequest.t(),
              state :: State.t()
            ) ::
              {:reply, reply, new_state}
              | {:reply, reply, new_state, timeout | :hibernate}
              | {:noreply, new_state}
              | {:noreply, new_state, timeout | :hibernate}
              | {:stop, reason, reply, new_state}
              | {:stop, reason, new_state}
            when reply: term, new_state: term, reason: term
  @callback kafka_server_offset_commit(
              request :: OffsetCommitRequest.t(),
              state :: State.t()
            ) ::
              {:reply, reply, new_state}
              | {:reply, reply, new_state, timeout | :hibernate}
              | {:noreply, new_state}
              | {:noreply, new_state, timeout | :hibernate}
              | {:stop, reason, reply, new_state}
              | {:stop, reason, new_state}
            when reply: term, new_state: term, reason: term
  @callback kafka_server_consumer_group_metadata(state :: State.t()) ::
              {:reply, reply, new_state}
              | {:reply, reply, new_state, timeout | :hibernate}
              | {:noreply, new_state}
              | {:noreply, new_state, timeout | :hibernate}
              | {:stop, reason, reply, new_state}
              | {:stop, reason, new_state}
            when reply: term, new_state: term, reason: term
  @callback kafka_server_metadata(topic :: binary, state :: State.t()) ::
              {:reply, reply, new_state}
              | {:reply, reply, new_state, timeout | :hibernate}
              | {:noreply, new_state}
              | {:noreply, new_state, timeout | :hibernate}
              | {:stop, reason, reply, new_state}
              | {:stop, reason, new_state}
            when reply: term, new_state: term, reason: term
  @callback kafka_server_join_group(
              JoinGroupRequest.t(),
              network_timeout :: integer,
              state :: State.t()
            ) ::
              {:reply, reply, new_state}
              | {:reply, reply, new_state, timeout | :hibernate}
              | {:noreply, new_state}
              | {:noreply, new_state, timeout | :hibernate}
              | {:stop, reason, reply, new_state}
              | {:stop, reason, new_state}
            when reply: term, new_state: term, reason: term
  @callback kafka_server_sync_group(
              SyncGroupRequest.t(),
              network_timeout :: integer,
              state :: State.t()
            ) ::
              {:reply, reply, new_state}
              | {:reply, reply, new_state, timeout | :hibernate}
              | {:noreply, new_state}
              | {:noreply, new_state, timeout | :hibernate}
              | {:stop, reason, reply, new_state}
              | {:stop, reason, new_state}
            when reply: term, new_state: term, reason: term
  @callback kafka_server_leave_group(
              LeaveGroupRequest.t(),
              network_timeout :: integer,
              state :: State.t()
            ) ::
              {:reply, reply, new_state}
              | {:reply, reply, new_state, timeout | :hibernate}
              | {:noreply, new_state}
              | {:noreply, new_state, timeout | :hibernate}
              | {:stop, reason, reply, new_state}
              | {:stop, reason, new_state}
            when reply: term, new_state: term, reason: term
  @callback kafka_server_heartbeat(
              HeartbeatRequest.t(),
              network_timeout :: integer,
              state :: State.t()
            ) ::
              {:reply, reply, new_state}
              | {:reply, reply, new_state, timeout | :hibernate}
              | {:noreply, new_state}
              | {:noreply, new_state, timeout | :hibernate}
              | {:stop, reason, reply, new_state}
              | {:stop, reason, new_state}
            when reply: term, new_state: term, reason: term
  @callback kafka_server_create_topics(
              [CreateTopicsRequest.t()],
              network_timeout :: integer,
              state :: State.t()
            ) :: {:reply, reply, new_state}
            when reply: term, new_state: term
  @callback kafka_server_delete_topics(
              [String.t()],
              network_timeout :: integer,
              state :: State.t()
            ) :: {:reply, reply, new_state}
            when reply: term, new_state: term
  @callback kafka_server_api_versions(state :: State.t()) ::
              {:reply, reply, new_state}
            when reply: term, new_state: term
  @callback kafka_server_update_metadata(state :: State.t()) ::
              {:noreply, new_state}
              | {:noreply, new_state, timeout | :hibernate}
              | {:stop, reason :: term, new_state}
            when new_state: term
  @callback kafka_server_update_consumer_metadata(state :: State.t()) ::
              {:noreply, new_state}
              | {:noreply, new_state, timeout | :hibernate}
              | {:stop, reason :: term, new_state}
            when new_state: term

  # Default from GenServer
  @default_call_timeout 5_000

  # the timeout parameter is  used also for network requests, which
  # means that if we set the same value for the GenServer timeout,
  # it will always timeout first => we won't get the expected behaviour,
  # which is to have the GenServer.call answer with the timeout reply.
  # Instead, we get a GenServer timeout error.
  # To avoid this, we add here an "buffer" time that covers the time
  # needed to process the logic until the network request, and back from it.
  @overhead_timeout 2_000

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
    GenServer.call(server, request, timeout + @overhead_timeout)
  end

  defmacro __using__(_) do
    # credo:disable-for-next-line Credo.Check.Refactor.LongQuoteBlocks
    quote location: :keep do
      @behaviour KafkaEx.Server
      require Logger
      alias KafkaEx.NetworkClient
      alias KafkaEx.Protocol.Offset

      @retry_count 3
      @wait_time 10
      @min_bytes 1
      @max_bytes 1_000_000
      @metadata_update_interval 30_000
      @sync_timeout 1_000
      @ssl_options []

      def init([args]) do
        kafka_server_init([args])
      end

      def init([args, name]) do
        kafka_server_init([args, name])
      end

      def handle_call(:consumer_group, _from, state) do
        kafka_server_consumer_group(state)
      end

      def handle_call({:produce, produce_request}, _from, state) do
        kafka_server_produce(produce_request, state)
      end

      def handle_call({:fetch, fetch_request}, _from, state) do
        kafka_server_fetch(fetch_request, state)
      end

      def handle_call({:offset, topic, partition, time}, _from, state) do
        kafka_server_offset(topic, partition, time, state)
      end

      def handle_call({:offset_fetch, offset_fetch}, _from, state) do
        kafka_server_offset_fetch(offset_fetch, state)
      end

      def handle_call({:offset_commit, offset_commit_request}, _from, state) do
        kafka_server_offset_commit(offset_commit_request, state)
      end

      def handle_call({:consumer_group_metadata, _consumer_group}, _from, state) do
        kafka_server_consumer_group_metadata(state)
      end

      def handle_call({:metadata, topic}, _from, state) do
        kafka_server_metadata(topic, state)
      end

      def handle_call({:join_group, request, network_timeout}, _from, state) do
        kafka_server_join_group(request, network_timeout, state)
      end

      def handle_call({:sync_group, request, network_timeout}, _from, state) do
        kafka_server_sync_group(request, network_timeout, state)
      end

      def handle_call({:leave_group, request, network_timeout}, _from, state) do
        kafka_server_leave_group(request, network_timeout, state)
      end

      def handle_call({:heartbeat, request, network_timeout}, _from, state) do
        kafka_server_heartbeat(request, network_timeout, state)
      end

      def handle_call({:create_topics, requests, network_timeout}, _from, state) do
        kafka_server_create_topics(requests, network_timeout, state)
      end

      def handle_call({:delete_topics, topics, network_timeout}, _from, state) do
        kafka_server_delete_topics(topics, network_timeout, state)
      end

      def handle_call({:api_versions}, _from, state) do
        kafka_server_api_versions(state)
      end

      def handle_info(:update_metadata, state) do
        kafka_server_update_metadata(state)
      end

      def handle_info(:update_consumer_metadata, state) do
        kafka_server_update_consumer_metadata(state)
      end

      def handle_info(_, state) do
        {:noreply, state}
      end

      def terminate(reason, state) do
        Logger.log(
          :debug,
          "Shutting down worker #{inspect(state.worker_name)}, reason: #{inspect(reason)}"
        )

        if state.event_pid do
          :gen_event.stop(state.event_pid)
        end

        Enum.each(state.brokers, fn broker ->
          NetworkClient.close_socket(broker.socket)
        end)
      end

      # KakfaEx.Server behavior default implementations
      # This needs a refactor, but for now make credo pass:
      # credo:disable-for-next-line Credo.Check.Refactor.CyclomaticComplexity
      def kafka_server_produce(
            produce_request,
            %State{metadata: metadata} = state
          ) do
        correlation_id = state.correlation_id + 1

        produce_request = default_partitioner().assign_partition(produce_request, metadata)

        produce_request_data =
          try do
            Produce.create_request(
              correlation_id,
              Config.client_id(),
              produce_request
            )
          rescue
            e in FunctionClauseError -> nil
          end

        case produce_request_data do
          nil ->
            {:reply, {:error, "Invalid produce request"}, state}

          _ ->
            kafka_server_produce_send_request(
              correlation_id,
              produce_request,
              produce_request_data,
              state
            )
        end
      end

      # credo:disable-for-next-line Credo.Check.Refactor.CyclomaticComplexity
      def kafka_server_produce_send_request(
            correlation_id,
            produce_request,
            produce_request_data,
            state
          ) do
        {broker, state, corr_id} =
          case MetadataResponse.broker_for_topic(
                 state.metadata,
                 state.brokers,
                 produce_request.topic,
                 produce_request.partition
               ) do
            nil ->
              {retrieved_corr_id, _} =
                retrieve_metadata(
                  state.brokers,
                  state.correlation_id,
                  config_sync_timeout(),
                  produce_request.topic,
                  state.api_versions
                )

              state = update_metadata(%{state | correlation_id: retrieved_corr_id})

              {
                MetadataResponse.broker_for_topic(
                  state.metadata,
                  state.brokers,
                  produce_request.topic,
                  produce_request.partition
                ),
                state,
                retrieved_corr_id
              }

            broker ->
              {broker, state, correlation_id}
          end

        response =
          case broker do
            nil ->
              Logger.log(
                :error,
                "kafka_server_produce_send_request: leader for topic #{produce_request.topic}/#{produce_request.partition} is not available"
              )

              :leader_not_available

            broker ->
              case produce_request.required_acks do
                0 ->
                  NetworkClient.send_async_request(broker, produce_request_data)

                _ ->
                  response =
                    broker
                    |> NetworkClient.send_sync_request(
                      produce_request_data,
                      config_sync_timeout()
                    )
                    |> case do
                      {:error, reason} -> reason
                      response -> Produce.parse_response(response)
                    end

                  # credo:disable-for-next-line Credo.Check.Refactor.Nesting
                  case response do
                    [
                      %KafkaEx.Protocol.Produce.Response{
                        partitions: [%{error_code: :no_error, offset: offset}],
                        topic: topic
                      }
                    ]
                    when offset != nil ->
                      {:ok, offset}

                    _ ->
                      {:error, response}
                  end
              end
          end

        state = %{state | correlation_id: corr_id + 1}
        {:reply, response, state}
      end

      def kafka_server_offset(topic, partition, time, state) do
        offset_request =
          Offset.create_request(
            state.correlation_id,
            Config.client_id(),
            topic,
            partition,
            time
          )

        {broker, state} =
          case MetadataResponse.broker_for_topic(
                 state.metadata,
                 state.brokers,
                 topic,
                 partition
               ) do
            nil ->
              state = update_metadata(state)

              {MetadataResponse.broker_for_topic(
                 state.metadata,
                 state.brokers,
                 topic,
                 partition
               ), state}

            broker ->
              {broker, state}
          end

        {response, state} =
          case broker do
            nil ->
              Logger.log(
                :error,
                "kafka_server_offset: leader for topic #{topic}/#{partition} is not available"
              )

              {:topic_not_found, state}

            _ ->
              response =
                broker
                |> NetworkClient.send_sync_request(
                  offset_request,
                  config_sync_timeout()
                )
                |> case do
                  {:error, reason} -> {:error, reason}
                  response -> Offset.parse_response(response)
                end

              state = increment_state_correlation_id(state)
              {response, state}
          end

        {:reply, response, state}
      end

      def kafka_server_metadata(topic, state) do
        {correlation_id, metadata} =
          retrieve_metadata(
            state.brokers,
            state.correlation_id,
            config_sync_timeout(),
            topic,
            state.api_versions
          )

        updated_state = %{
          state
          | metadata: metadata,
            correlation_id: correlation_id
        }

        {:reply, metadata, updated_state}
      end

      def kafka_server_update_metadata(state) do
        {:noreply, update_metadata(state)}
      end

      def update_metadata(state) do
        {correlation_id, metadata} =
          retrieve_metadata(
            state.brokers,
            state.correlation_id,
            config_sync_timeout(),
            nil,
            state.api_versions
          )

        metadata_brokers =
          metadata.brokers
          |> Enum.map(&%{&1 | is_controller: &1.node_id == metadata.controller_id})

        brokers =
          state.brokers
          |> remove_stale_brokers(metadata_brokers)
          |> add_new_brokers(metadata_brokers, state.ssl_options, state.use_ssl)

        %{
          state
          | metadata: metadata,
            brokers: brokers,
            correlation_id: correlation_id + 1
        }
      end

      # credo:disable-for-next-line Credo.Check.Refactor.FunctionArity
      def retrieve_metadata(
            brokers,
            correlation_id,
            sync_timeout,
            topic \\ [],
            server_api_versions \\ [:unsupported]
          ) do
        retrieve_metadata(
          brokers,
          correlation_id,
          sync_timeout,
          topic,
          @retry_count,
          0,
          server_api_versions
        )
      end

      # credo:disable-for-next-line Credo.Check.Refactor.FunctionArity
      def retrieve_metadata(
            brokers,
            correlation_id,
            sync_timeout,
            topic,
            retry,
            error_code,
            server_api_versions \\ [:unsupported]
          ) do
        api_version = Metadata.api_version(server_api_versions)

        retrieve_metadata_with_version(
          brokers,
          correlation_id,
          sync_timeout,
          topic,
          retry,
          error_code,
          api_version
        )
      end

      # credo:disable-for-next-line Credo.Check.Refactor.FunctionArity
      def retrieve_metadata_with_version(
            _,
            correlation_id,
            _sync_timeout,
            topic,
            0,
            error_code,
            server_api_versions
          ) do
        Logger.log(
          :error,
          "Metadata request for topic #{inspect(topic)} failed with error_code #{inspect(error_code)}"
        )

        {correlation_id, %Metadata.Response{}}
      end

      # credo:disable-for-next-line Credo.Check.Refactor.FunctionArity
      def retrieve_metadata_with_version(
            brokers,
            correlation_id,
            sync_timeout,
            topic,
            retry,
            _error_code,
            api_version
          ) do
        metadata_request =
          Metadata.create_request(
            correlation_id,
            Config.client_id(),
            topic,
            api_version
          )

        data = first_broker_response(metadata_request, brokers, sync_timeout)

        if data do
          response = Metadata.parse_response(data, api_version)

          case Enum.find(
                 response.topic_metadatas,
                 &(&1.error_code == :leader_not_available)
               ) do
            nil ->
              {correlation_id + 1, response}

            topic_metadata ->
              :timer.sleep(300)

              retrieve_metadata_with_version(
                brokers,
                correlation_id + 1,
                sync_timeout,
                topic,
                retry - 1,
                topic_metadata.error_code,
                api_version
              )
          end
        else
          message = "Unable to fetch metadata from any brokers. Timeout is #{sync_timeout}."

          Logger.log(:error, message)
          raise message
          :no_metadata_available
        end
      end

      defoverridable kafka_server_produce: 2,
                     kafka_server_offset: 4,
                     kafka_server_metadata: 2,
                     kafka_server_update_metadata: 1

      defp kafka_common_init(args, name) do
        use_ssl = Keyword.get(args, :use_ssl, false)
        ssl_options = Keyword.get(args, :ssl_options, [])

        uris = Keyword.get(args, :uris, [])

        metadata_update_interval =
          Keyword.get(
            args,
            :metadata_update_interval,
            @metadata_update_interval
          )

        brokers =
          for {host, port} <- uris do
            connect_broker(host, port, ssl_options, use_ssl)
          end

        check_brokers_sockets!(brokers)

        {correlation_id, metadata} =
          try do
            retrieve_metadata(
              brokers,
              0,
              config_sync_timeout()
            )
          rescue
            e ->
              sleep_for_reconnect()
              Kernel.reraise(e, __STACKTRACE__)
          end

        state = %State{
          metadata: metadata,
          brokers: brokers,
          correlation_id: correlation_id,
          metadata_update_interval: metadata_update_interval,
          ssl_options: ssl_options,
          use_ssl: use_ssl,
          worker_name: name,
          api_versions: [:unsupported]
        }

        state = update_metadata(state)

        {:ok, _} =
          :timer.send_interval(
            state.metadata_update_interval,
            :update_metadata
          )

        state
      end

      defp sleep_for_reconnect() do
        Process.sleep(Application.get_env(:kafka_ex, :sleep_for_reconnect, 400))
      end

      defp check_brokers_sockets!(brokers) do
        any_socket_opened =
          brokers
          |> Enum.any?(fn %Broker{socket: socket} -> not is_nil(socket) end)

        if not any_socket_opened do
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
          | client_id: Config.client_id(),
            correlation_id: state.correlation_id
        }
      end

      # gets the broker for a given partition, updating metadata if necessary
      # returns {broker, maybe_updated_state}
      defp broker_for_partition_with_update(state, topic, partition) do
        case State.broker_for_partition(state, topic, partition) do
          nil ->
            updated_state = update_metadata(state)

            {
              State.broker_for_partition(updated_state, topic, partition),
              updated_state
            }

          broker ->
            {broker, state}
        end
      end

      # assumes module.create_request(request) and module.parse_response
      # both work
      defp network_request(request, module, state) do
        {broker, updated_state} =
          broker_for_partition_with_update(
            state,
            request.topic,
            request.partition
          )

        case broker do
          nil ->
            Logger.error(fn ->
              "network_request: leader for topic #{request.topic}/#{request.partition} is not available"
            end)

            {{:error, :topic_not_found}, updated_state}

          _ ->
            wire_request =
              request
              |> client_request(updated_state)
              |> module.create_request

            response =
              broker
              |> NetworkClient.send_sync_request(
                wire_request,
                config_sync_timeout()
              )
              |> case do
                {:error, reason} ->
                  {:error, reason}

                response ->
                  try do
                    module.parse_response(response)
                  rescue
                    _ ->
                      Logger.error(
                        "Failed to parse a response from the server: #{inspect(response)}"
                      )

                      Kernel.reraise(
                        "Parse error during #{inspect(module)}.parse_response. Couldn't parse: #{inspect(response)}",
                        __STACKTRACE__
                      )
                  end
              end

            state_out = State.increment_correlation_id(updated_state)
            {response, state_out}
        end
      end

      defp remove_stale_brokers(brokers, metadata_brokers) do
        {brokers_to_keep, brokers_to_remove} =
          apply(Enum, :partition, [
            brokers,
            fn broker ->
              Enum.find_value(
                metadata_brokers,
                &(broker.node_id == -1 ||
                    (broker.node_id == &1.node_id && broker.socket &&
                       Socket.info(broker.socket)))
              )
            end
          ])

        case length(brokers_to_keep) do
          0 ->
            brokers_to_remove

          _ ->
            Enum.each(brokers_to_remove, fn broker ->
              Logger.log(
                :debug,
                "Closing connection to broker #{broker.node_id}: #{inspect(broker.host)} on port #{inspect(broker.port)}"
              )

              NetworkClient.close_socket(broker.socket)
            end)

            brokers_to_keep
        end
      end

      defp add_new_brokers(brokers, [], _, _), do: brokers

      defp add_new_brokers(
             brokers,
             [metadata_broker | metadata_brokers],
             ssl_options,
             use_ssl
           ) do
        case Enum.find(brokers, &(metadata_broker.node_id == &1.node_id)) do
          nil ->
            Logger.log(
              :debug,
              "Establishing connection to broker #{metadata_broker.node_id}: #{inspect(metadata_broker.host)} on port #{inspect(metadata_broker.port)}"
            )

            add_new_brokers(
              [
                %{
                  metadata_broker
                  | socket:
                      NetworkClient.create_socket(
                        metadata_broker.host,
                        metadata_broker.port,
                        ssl_options,
                        use_ssl
                      )
                }
                | brokers
              ],
              metadata_brokers,
              ssl_options,
              use_ssl
            )

          _ ->
            add_new_brokers(brokers, metadata_brokers, ssl_options, use_ssl)
        end
      end

      defp first_broker_response(request, brokers, timeout) do
        brokers
        |> Enum.shuffle()
        |> Enum.find_value(fn broker ->
          if Broker.connected?(broker) do
            # credo:disable-for-next-line Credo.Check.Refactor.Nesting
            case NetworkClient.send_sync_request(broker, request, timeout) do
              {:error, _} -> nil
              response -> response
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

      defp increment_state_correlation_id(%_{correlation_id: correlation_id} = state) do
        %{state | correlation_id: correlation_id + 1}
      end
    end
  end
end
