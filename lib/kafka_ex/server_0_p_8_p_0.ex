defmodule KafkaEx.Server0P8P0 do
  use KafkaEx.Server
  alias KafkaEx.Protocol.{Fetch, Metadata, Produce}
  alias KafkaEx.Server.State

  def kafka_server_init([args]) do
    kafka_server_init([args, self])
  end

  def kafka_server_init([args, name]) do
    uris = Keyword.get(args, :uris, [])
    metadata_update_interval = Keyword.get(args, :metadata_update_interval, @metadata_update_interval)
    brokers = Enum.map(uris, fn({host, port}) -> %Metadata.Broker{host: host, port: port, socket: KafkaEx.NetworkClient.create_socket(host, port)} end)
    sync_timeout = Keyword.get(args, :sync_timeout, Application.get_env(:kafka_ex, :sync_timeout, @sync_timeout))
    {correlation_id, metadata} = retrieve_metadata(brokers, 0, sync_timeout)
    state = %State{metadata: metadata, brokers: brokers, correlation_id: correlation_id, metadata_update_interval: metadata_update_interval, worker_name: name, sync_timeout: sync_timeout}
    {:ok, _} = :timer.send_interval(state.metadata_update_interval, :update_metadata)

    {:ok, state}
  end

  def start_link(server_impl, args, name \\ __MODULE__)

  def start_link(server_impl, args, :no_name) do
    GenServer.start_link(__MODULE__, [server_impl, args])
  end

  def start_link(server_impl, args, name) do
    GenServer.start_link(__MODULE__, [server_impl, args, name], [name: name])
  end

  def kafka_server_produce(produce_request, state) do
    correlation_id = state.correlation_id + 1
    produce_request_data = Produce.create_request(correlation_id, @client_id, produce_request)
    {broker, updated_state} = case Metadata.Response.broker_for_topic(state.metadata, state.brokers, produce_request.topic, produce_request.partition) do
      nil    ->
        {retrieved_corr_id, _} = retrieve_metadata(state.brokers, state.correlation_id, state.sync_timeout, produce_request.topic)
        new_state = %{update_metadata(state) | correlation_id: retrieved_corr_id}
        {Metadata.Response.broker_for_topic(new_state.metadata, new_state.brokers, produce_request.topic, produce_request.partition), new_state}
      broker -> {broker, state}
    end

    response = case broker do
      nil    ->
        Logger.log(:error, "Leader for topic #{produce_request.topic} is not available")
        :leader_not_available
      broker -> case produce_request.required_acks do
        0 ->  KafkaEx.NetworkClient.send_async_request(broker, produce_request_data)
        _ -> KafkaEx.NetworkClient.send_sync_request(broker, produce_request_data, state.sync_timeout) |> Produce.parse_response
      end
    end

    state1 = %{updated_state | correlation_id: correlation_id + 1}
    {:reply, response, state1}
  end

  def kafka_server_fetch(topic, partition, offset, wait_time, min_bytes, max_bytes, auto_commit, state) do
    {response, state} = fetch(topic, partition, offset, wait_time, min_bytes, max_bytes, state, auto_commit)

    {:reply, response, state}
  end

  def kafka_server_offset_fetch( _, _state), do: raise "Offset Fetch is not supported in 0.8.0 version of kafka"
  def kafka_server_offset_commit( _, _state), do: raise "Offset Commit is not supported in 0.8.0 version of kafka"
  def kafka_server_consumer_group( _state), do: raise "Consumer Group is not supported in 0.8.0 version of kafka"
  def kafka_server_consumer_group_metadata(_state), do: raise "Consumer Group Metadata is not supported in 0.8.0 version of kafka"
  def kafka_server_join_group( _, _, _state), do: raise "Join Group is not supported in 0.8.0 version of kafka"
  def kafka_server_sync_group(_, _, _, _, _state), do: raise "Sync Group is not supported in 0.8.0 version of kafka"
  def kafka_server_heartbeat(_, _, _, _state), do: raise "Heartbeat is not supported in 0.8.0 version of kafka"
  def kafka_server_update_consumer_metadata(_state), do: raise "Consumer Group Metadata is not supported in 0.8.0 version of kafka"

  def kafka_server_start_streaming(topic, partition, offset, handler, auto_commit, state) do
    {response, state} = fetch(topic, partition, offset, @wait_time, @min_bytes, @max_bytes, state, auto_commit)
    offset = case response do
               :topic_not_found ->
                 offset
               _ ->
                 message = response |> hd |> Map.get(:partitions) |> hd
                 Enum.each(message.message_set, fn(message_set) -> GenEvent.notify(state.event_pid, message_set) end)
                 case message.last_offset do
                   nil         -> offset
                   last_offset -> last_offset + 1
                 end
             end

    Process.send_after(self, {:start_streaming, topic, partition, offset, handler, auto_commit}, 500)

    {:noreply, state}
  end

  defp fetch(topic, partition, offset, wait_time, min_bytes, max_bytes, state, _auto_commit) do
    fetch_request = Fetch.create_request(state.correlation_id, @client_id, topic, partition, offset, wait_time, min_bytes, max_bytes)
    {broker, state} = case Metadata.Response.broker_for_topic(state.metadata, state.brokers, topic, partition) do
      nil    ->
        updated_state = update_metadata(state)
        {Metadata.Response.broker_for_topic(state.metadata, state.brokers, topic, partition), updated_state}
      broker -> {broker, state}
    end

    case broker do
      nil ->
        Logger.log(:error, "Leader for topic #{topic} is not available")
        {:topic_not_found, state}
      _ ->
        response = broker
          |> KafkaEx.NetworkClient.send_sync_request(fetch_request, state.sync_timeout)
          |> Fetch.parse_response
        {response, %{state | correlation_id: state.correlation_id + 1}}
    end
  end
end
