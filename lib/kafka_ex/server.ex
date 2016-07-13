defmodule KafkaEx.Server do
  @moduledoc """
  Defines the KafkaEx.Server behavior that all Kafka API servers must implement, this module also provides some common callback functions that are injected into the servers that `use` it.
  """

  alias KafkaEx.Protocol.ConsumerMetadata
  alias KafkaEx.Protocol.Metadata
  alias KafkaEx.Protocol.Metadata.Broker
  alias KafkaEx.Protocol.Metadata.Response, as: MetadataResponse
  alias KafkaEx.Protocol.OffsetCommit.Request, as: OffsetCommitRequest
  alias KafkaEx.Protocol.OffsetFetch.Request, as: OffsetFetchRequest
  alias KafkaEx.Protocol.Produce
  alias KafkaEx.Protocol.Produce.Request, as: ProduceRequest

  defmodule State do
    @moduledoc false

    defstruct(metadata: %Metadata.Response{},
    server_impl: nil,
    brokers: [],
    event_pid: nil,
    stream_timer: nil,
    consumer_metadata: %ConsumerMetadata.Response{},
    correlation_id: 0,
    consumer_group: nil,
    metadata_update_interval: nil,
    consumer_group_update_interval: nil,
    worker_name: KafkaEx.Server,
    sync_timeout: nil)
  end

  @callback kafka_server_init(args :: [term]) ::
    {:ok, state} |
    {:ok, state, timeout | :hibernate} |
    :ignore |
    {:stop, reason :: any} when state: any
  @callback kafka_server_produce(request :: ProduceRequest.t, state :: State.t) ::
    {:reply, reply, new_state} |
    {:reply, reply, new_state, timeout | :hibernate} |
    {:noreply, new_state} |
    {:noreply, new_state, timeout | :hibernate} |
    {:stop, reason, reply, new_state} |
    {:stop, reason, new_state} when reply: term, new_state: term, reason: term
  @callback kafka_server_consumer_group(state :: State.t) ::
    {:reply, reply, new_state} |
    {:reply, reply, new_state, timeout | :hibernate} |
    {:noreply, new_state} |
    {:noreply, new_state, timeout | :hibernate} |
    {:stop, reason, reply, new_state} |
    {:stop, reason, new_state} when reply: term, new_state: term, reason: term
  @callback kafka_server_fetch(fetch_request :: FetchRequest.t, state :: State.t) ::
    {:reply, reply, new_state} |
    {:reply, reply, new_state, timeout | :hibernate} |
    {:noreply, new_state} |
    {:noreply, new_state, timeout | :hibernate} |
    {:stop, reason, reply, new_state} |
    {:stop, reason, new_state} when reply: term, new_state: term, reason: term
  @callback kafka_server_offset(topic :: binary, parition :: integer, time :: integer | :latest | :earliest, state :: State.t) ::
    {:reply, reply, new_state} |
    {:reply, reply, new_state, timeout | :hibernate} |
    {:noreply, new_state} |
    {:noreply, new_state, timeout | :hibernate} |
    {:stop, reason, reply, new_state} |
    {:stop, reason, new_state} when reply: term, new_state: term, reason: term
  @callback kafka_server_offset_fetch(request :: OffsetFetchRequest.t, state :: State.t) ::
    {:reply, reply, new_state} |
    {:reply, reply, new_state, timeout | :hibernate} |
    {:noreply, new_state} |
    {:noreply, new_state, timeout | :hibernate} |
    {:stop, reason, reply, new_state} |
    {:stop, reason, new_state} when reply: term, new_state: term, reason: term
  @callback kafka_server_offset_commit(request :: OffsetCommitRequest.t, state :: State.t) ::
    {:reply, reply, new_state} |
    {:reply, reply, new_state, timeout | :hibernate} |
    {:noreply, new_state} |
    {:noreply, new_state, timeout | :hibernate} |
    {:stop, reason, reply, new_state} |
    {:stop, reason, new_state} when reply: term, new_state: term, reason: term
  @callback kafka_server_consumer_group_metadata(state :: State.t) ::
    {:reply, reply, new_state} |
    {:reply, reply, new_state, timeout | :hibernate} |
    {:noreply, new_state} |
    {:noreply, new_state, timeout | :hibernate} |
    {:stop, reason, reply, new_state} |
    {:stop, reason, new_state} when reply: term, new_state: term, reason: term
  @callback kafka_server_metadata(topic :: binary, state :: State.t) ::
    {:reply, reply, new_state} |
    {:reply, reply, new_state, timeout | :hibernate} |
    {:noreply, new_state} |
    {:noreply, new_state, timeout | :hibernate} |
    {:stop, reason, reply, new_state} |
    {:stop, reason, new_state} when reply: term, new_state: term, reason: term
  @callback kafka_server_join_group(topics :: [binary], session_timeout :: integer, state :: State.t) ::
    {:reply, reply, new_state} |
    {:reply, reply, new_state, timeout | :hibernate} |
    {:noreply, new_state} |
    {:noreply, new_state, timeout | :hibernate} |
    {:stop, reason, reply, new_state} |
    {:stop, reason, new_state} when reply: term, new_state: term, reason: term
  @callback kafka_server_sync_group(group_name :: binary, generation_id :: integer, member_id :: binary, assignments :: [binary] , state :: State.t) ::
    {:reply, reply, new_state} |
    {:reply, reply, new_state, timeout | :hibernate} |
    {:noreply, new_state} |
    {:noreply, new_state, timeout | :hibernate} |
    {:stop, reason, reply, new_state} |
    {:stop, reason, new_state} when reply: term, new_state: term, reason: term
  @callback kafka_server_heartbeat(group_name :: binary, generation_id :: integer, member_id :: integer, state :: State.t) ::
    {:reply, reply, new_state} |
    {:reply, reply, new_state, timeout | :hibernate} |
    {:noreply, new_state} |
    {:noreply, new_state, timeout | :hibernate} |
    {:stop, reason, reply, new_state} |
    {:stop, reason, new_state} when reply: term, new_state: term, reason: term
  @callback kafka_server_create_stream(handler :: term, handler_init :: term, state :: State.t) ::
    {:reply, reply, new_state} |
    {:reply, reply, new_state, timeout | :hibernate} |
    {:noreply, new_state} |
    {:noreply, new_state, timeout | :hibernate} |
    {:stop, reason, reply, new_state} |
    {:stop, reason, new_state} when reply: term, new_state: term, reason: term
  @callback kafka_server_start_streaming(fetch_request :: FetchRequest.t, state :: State.t) ::
    {:noreply, new_state} |
    {:noreply, new_state, timeout | :hibernate} |
    {:stop, reason :: term, new_state} when new_state: term
  @callback kafka_server_stop_streaming(state :: State.t) ::
    {:noreply, new_state} |
    {:noreply, new_state, timeout | :hibernate} |
    {:stop, reason :: term, new_state} when new_state: term
  @callback kafka_server_update_metadata(state :: State.t) ::
    {:noreply, new_state} |
    {:noreply, new_state, timeout | :hibernate} |
    {:stop, reason :: term, new_state} when new_state: term
  @callback kafka_server_update_consumer_metadata(state :: State.t) ::
    {:noreply, new_state} |
    {:noreply, new_state, timeout | :hibernate} |
    {:stop, reason :: term, new_state} when new_state: term


  defmacro __using__(_) do
    quote location: :keep do
      @behaviour KafkaEx.Server
      require Logger
      alias KafkaEx.NetworkClient
      alias KafkaEx.Protocol.Offset

      @client_id "kafka_ex"
      @retry_count 3
      @wait_time 10
      @min_bytes 1
      @max_bytes 1_000_000
      @metadata_update_interval       30_000
      @sync_timeout                   1_000

      def init([server_impl, args]) do
        {:ok, state} = server_impl.kafka_server_init([args])
        {:ok, %{callback_state: state, callback_module: server_impl}}
      end

      def init([server_impl, args, name]) do
        {:ok, state} = server_impl.kafka_server_init([args, name])
        {:ok, %{callback_state: state, callback_module: server_impl}}
      end

      def handle_call(:consumer_group, _from, %{callback_state: callback_state, callback_module: server_impl} = state) do
        {:reply, response, callback_state} = server_impl.kafka_server_consumer_group(callback_state)
        {:reply, response, %{callback_state: callback_state, callback_module: server_impl}}
      end

      def handle_call({:produce, produce_request}, _from, %{callback_state: callback_state, callback_module: server_impl} = state) do
        {:reply, response, callback_state} = server_impl.kafka_server_produce(produce_request, callback_state)
        {:reply, response, %{callback_state: callback_state, callback_module: server_impl}}
      end

      def handle_call({:fetch, fetch_request}, _from, %{callback_state: callback_state, callback_module: server_impl} = state) do
        {:reply, response, callback_state} = server_impl.kafka_server_fetch(fetch_request, callback_state)
        {:reply, response, %{callback_state: callback_state, callback_module: server_impl}}
      end

      def handle_call({:offset, topic, partition, time}, _from, %{callback_state: callback_state, callback_module: server_impl} = state) do
        {:reply, response, callback_state} = server_impl.kafka_server_offset(topic, partition, time, callback_state)
        {:reply, response, %{callback_state: callback_state, callback_module: server_impl}}
      end

      def handle_call({:offset_fetch, offset_fetch}, _from, %{callback_state: callback_state, callback_module: server_impl} = state) do
        {:reply, response, callback_state} = server_impl.kafka_server_offset_fetch(offset_fetch, callback_state)
        {:reply, response, %{callback_state: callback_state, callback_module: server_impl}}
      end

      def handle_call({:offset_commit, offset_commit_request}, _from, %{callback_state: callback_state, callback_module: server_impl} = state) do
        {:reply, response, callback_state} = server_impl.kafka_server_offset_commit(offset_commit_request, callback_state)
        {:reply, response, %{callback_state: callback_state, callback_module: server_impl}}
      end

      def handle_call({:consumer_group_metadata, _consumer_group}, _from, %{callback_state: callback_state, callback_module: server_impl} = state) do
        {:reply, response, callback_state} = server_impl.kafka_server_consumer_group_metadata(callback_state)
        {:reply, response, %{callback_state: callback_state, callback_module: server_impl}}
      end

      def handle_call({:metadata, topic}, _from, %{callback_state: callback_state, callback_module: server_impl} = state) do
        {:reply, response, callback_state} = server_impl.kafka_server_metadata(topic, callback_state)
        {:reply, response, %{callback_state: callback_state, callback_module: server_impl}}
      end

      def handle_call({:join_group, topics, session_timeout}, _from, %{callback_state: callback_state, callback_module: server_impl} = state) do
        {:reply, response, callback_state} = server_impl.kafka_server_join_group(topics, session_timeout,callback_state)
        {:reply, response, %{callback_state: callback_state, callback_module: server_impl}}
      end

      def handle_call({:sync_group, group_name, generation_id, member_id, assignments}, _from, %{callback_state: callback_state, callback_module: server_impl} = state) do
        {:reply, response, callback_state} = server_impl.kafka_server_sync_group(group_name, generation_id, member_id, assignments, callback_state)
        {:reply, response, %{callback_state: callback_state, callback_module: server_impl}}
      end

      def handle_call({:heartbeat, group_name, generation_id, member_id}, _from, %{callback_state: callback_state, callback_module: server_impl} = state) do
        {:reply, response, callback_state} = server_impl.kafka_server_heartbeat(group_name, generation_id, member_id, callback_state)
        {:reply, response, %{callback_state: callback_state, callback_module: server_impl}}
      end

      def handle_call({:create_stream, handler, handler_init}, _from, %{callback_state: callback_state, callback_module: server_impl} = state) do
        {:reply, response, callback_state} = server_impl.kafka_server_create_stream(handler, handler_init, callback_state)
        {:reply, response, %{callback_state: callback_state, callback_module: server_impl}}
      end

      def handle_info({:start_streaming, fetch_request}, %{callback_state: callback_state, callback_module: server_impl} = state) do
        {:noreply, callback_state} = server_impl.kafka_server_start_streaming(fetch_request, callback_state)
        {:noreply, %{callback_state: callback_state, callback_module: server_impl}}
      end

      def handle_info(:stop_streaming, %{callback_state: callback_state, callback_module: server_impl} = state) do
        {:noreply, callback_state} = server_impl.kafka_server_stop_streaming(callback_state)
        callback_state = case callback_state.stream_timer do
          nil -> callback_state
          ref -> Process.cancel_timer(ref)
          %{callback_state | stream_timer: nil}
        end
        {:noreply, %{callback_state: callback_state, callback_module: server_impl}}
      end

      def handle_info(:update_metadata, %{callback_state: callback_state, callback_module: server_impl} = state) do
        {:noreply, callback_state} = server_impl.kafka_server_update_metadata(callback_state)
        {:noreply, %{callback_state: callback_state, callback_module: server_impl}}
      end

      def handle_info(:update_consumer_metadata, %{callback_state: callback_state, callback_module: server_impl} = state) do
        {:noreply, callback_state} = server_impl.kafka_server_update_consumer_metadata(callback_state)
        {:noreply, %{callback_state: callback_state, callback_module: server_impl}}
      end

      def handle_info(_, %{callback_state: callback_state, callback_module: server_impl} = state) do
        {:noreply, state}
      end


      def terminate(_, %{callback_state: callback_state, callback_module: server_impl} = state) do
        Logger.log(:debug, "Shutting down worker #{inspect callback_state.worker_name}")
        if callback_state.event_pid do
          GenEvent.stop(callback_state.event_pid)
        end
        Enum.each(callback_state.brokers, fn(broker) -> NetworkClient.close_socket(broker.socket) end)
      end

      # KakfaEx.Server behavior default implementations
      def kafka_server_produce(produce_request, state) do
        correlation_id = state.correlation_id + 1
        produce_request_data = Produce.create_request(correlation_id, @client_id, produce_request)
        {broker, state, corr_id} = case MetadataResponse.broker_for_topic(state.metadata, state.brokers, produce_request.topic, produce_request.partition) do
          nil    ->
            {retrieved_corr_id, _} = retrieve_metadata(state.brokers, state.correlation_id, state.sync_timeout, produce_request.topic)
            state = %{update_metadata(state) | correlation_id: retrieved_corr_id}
            {
              MetadataResponse.broker_for_topic(state.metadata, state.brokers, produce_request.topic, produce_request.partition),
              state,
              retrieved_corr_id
            }
          broker -> {broker, state, correlation_id}
        end

        response = case broker do
          nil    ->
            Logger.log(:error, "Leader for topic #{produce_request.topic} is not available")
            :leader_not_available
          broker -> case produce_request.required_acks do
            0 ->  NetworkClient.send_async_request(broker, produce_request_data)
            _ -> broker
            |> NetworkClient.send_sync_request(produce_request_data, state.sync_timeout)
            |> Produce.parse_response
          end
        end
        state = %{state | correlation_id: corr_id + 1}
        {:reply, response, state}
      end

      def kafka_server_offset(topic, partition, time, state) do
        offset_request = Offset.create_request(state.correlation_id, @client_id, topic, partition, time)
        {broker, state} = case MetadataResponse.broker_for_topic(state.metadata, state.brokers, topic, partition) do
          nil    ->
            state = update_metadata(state)
            {MetadataResponse.broker_for_topic(state.metadata, state.brokers, topic, partition), state}
          broker -> {broker, state}
        end

        {response, state} = case broker do
          nil ->
            Logger.log(:error, "Leader for topic #{topic} is not available")
            {:topic_not_found, state}
          _ ->
            response = broker
             |> NetworkClient.send_sync_request(offset_request, state.sync_timeout)
             |> Offset.parse_response
            state = %{state | correlation_id: state.correlation_id + 1}
            {response, state}
        end

        {:reply, response, state}
      end

      def kafka_server_metadata(topic, state) do
        {correlation_id, metadata} = retrieve_metadata(state.brokers, state.correlation_id, state.sync_timeout, topic)
        updated_state = %{state | metadata: metadata, correlation_id: correlation_id}
        {:reply, metadata, updated_state}
      end

      def kafka_server_create_stream(handler, handler_init, state) do
        new_state = if state.event_pid && Process.alive?(state.event_pid) do
          Logger.log(:warn, "'#{state.worker_name}' already streaming handler '#{handler}'")
          state
        else
          {:ok, event_pid}  = GenEvent.start_link
          updated_state = %{state | event_pid: event_pid}
          :ok = GenEvent.add_handler(updated_state.event_pid, handler, handler_init)
          updated_state
        end
        {:reply, GenEvent.stream(new_state.event_pid), new_state}
      end

      def kafka_server_stop_streaming(state) do
        Logger.log(:debug, "Stopped worker #{inspect state.worker_name} from streaming")
        GenEvent.stop(state.event_pid)
        {:noreply, %{state | event_pid: nil}}
      end

      def kafka_server_update_metadata(state) do
        {:noreply, update_metadata(state)}
      end

      def update_metadata(state) do
        {correlation_id, metadata} = retrieve_metadata(state.brokers, state.correlation_id, state.sync_timeout)
        metadata_brokers = metadata.brokers
        brokers = state.brokers
          |> remove_stale_brokers(metadata_brokers)
          |> add_new_brokers(metadata_brokers)
        %{state | metadata: metadata, brokers: brokers, correlation_id: correlation_id + 1}
      end

      def retrieve_metadata(brokers, correlation_id, sync_timeout, topic \\ []), do: retrieve_metadata(brokers, correlation_id, sync_timeout, topic, @retry_count, 0)
      def retrieve_metadata(_, correlation_id, _sync_timeout, topic, 0, error_code) do
        Logger.log(:error, "Metadata request for topic #{inspect topic} failed with error_code #{inspect error_code}")
        {correlation_id, %Metadata.Response{}}
      end
      def retrieve_metadata(brokers, correlation_id, sync_timeout, topic, retry, _error_code) do
        metadata_request = Metadata.create_request(correlation_id, @client_id, topic)
        data = first_broker_response(metadata_request, brokers, sync_timeout)
        response = case data do
                     nil ->
                       Logger.log(:error, "Unable to fetch metadata from any brokers.  Timeout is #{sync_timeout}.")
                       raise "Unable to fetch metadata from any brokers.  Timeout is #{sync_timeout}."
                       :no_metadata_available
                     data ->
                       Metadata.parse_response(data)
                   end

                   case Enum.find(response.topic_metadatas, &(&1.error_code == :leader_not_available)) do
          nil  -> {correlation_id + 1, response}
          topic_metadata ->
            :timer.sleep(300)
            retrieve_metadata(brokers, correlation_id + 1, sync_timeout, topic, retry - 1, topic_metadata.error_code)
        end
      end

      defoverridable [
        kafka_server_produce: 2, kafka_server_offset: 4,
        kafka_server_metadata: 2, kafka_server_create_stream: 3,
        kafka_server_stop_streaming: 1, kafka_server_update_metadata: 1,
      ]

      defp remove_stale_brokers(brokers, metadata_brokers) do
        {brokers_to_keep, brokers_to_remove} = Enum.partition(brokers, fn(broker) ->
          Enum.find_value(metadata_brokers, &(broker.host == &1.host && broker.port == &1.port && broker.socket && Port.info(broker.socket)))
        end)
        case length(brokers_to_keep) do
          0 -> brokers_to_remove
          _ -> Enum.each(brokers_to_remove, fn(broker) ->
            Logger.log(:info, "Closing connection to broker #{inspect broker.host} on port #{inspect broker.port}")
            NetworkClient.close_socket(broker.socket)
          end)
            brokers_to_keep
        end
      end

      defp add_new_brokers(brokers, []), do: brokers
      defp add_new_brokers(brokers, [metadata_broker|metadata_brokers]) do
        case Enum.find(brokers, &(metadata_broker.host == &1.host && metadata_broker.port == &1.port)) do
          nil -> Logger.log(:info, "Establishing connection to broker #{inspect metadata_broker.host} on port #{inspect metadata_broker.port}")
            add_new_brokers([%{metadata_broker | socket: NetworkClient.create_socket(metadata_broker.host, metadata_broker.port)} | brokers], metadata_brokers)
          _ -> add_new_brokers(brokers, metadata_brokers)
        end
      end

      defp first_broker_response(request, brokers, sync_timeout) do
        Enum.find_value(brokers, fn(broker) ->
          if Broker.connected?(broker) do
            NetworkClient.send_sync_request(broker, request, sync_timeout)
          end
        end)
      end
    end
  end
end
