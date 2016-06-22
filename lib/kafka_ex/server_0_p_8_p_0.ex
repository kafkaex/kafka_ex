defmodule KafkaEx.Server0P8P0 do
  use KafkaEx.ServerBase

  @metadata_update_interval       30_000
  @sync_timeout                   1_000

  ### GenServer Callbacks

  def start_link(args, name \\ __MODULE__)

  def start_link(args, :no_name) do
    GenServer.start_link(__MODULE__, [args])
  end

  def start_link(args, name) do
    GenServer.start_link(__MODULE__, [args, name], [name: name])
  end

  def init([args]) do
    init([args, self])
  end

  def init([args, name]) do
    uris = Keyword.get(args, :uris, [])
    metadata_update_interval = Keyword.get(args, :metadata_update_interval, @metadata_update_interval)
    brokers = Enum.map(uris, fn({host, port}) -> %Proto.Metadata.Broker{host: host, port: port, socket: KafkaEx.NetworkClient.create_socket(host, port)} end)
    sync_timeout = Keyword.get(args, :sync_timeout, Application.get_env(:kafka_ex, :sync_timeout, @sync_timeout))
    {correlation_id, metadata} = retrieve_metadata(brokers, 0, sync_timeout)
    state = %State{metadata: metadata, brokers: brokers, correlation_id: correlation_id, metadata_update_interval: metadata_update_interval, worker_name: name, sync_timeout: sync_timeout}
    {:ok, _} = :timer.send_interval(state.metadata_update_interval, :update_metadata)

    {:ok, state}
  end

  def handle_call({:produce, produce_request}, _from, state) do
    correlation_id = state.correlation_id + 1
    produce_request_data = Proto.Produce.create_request(correlation_id, client_id, produce_request)
    {broker, state} = case Proto.Metadata.Response.broker_for_topic(state.metadata, state.brokers, produce_request.topic, produce_request.partition) do
      nil    ->
        {correlation_id, _} = retrieve_metadata(state.brokers, state.correlation_id, state.sync_timeout, produce_request.topic)
        state = %{update_metadata(state) | correlation_id: correlation_id}
        {Proto.Metadata.Response.broker_for_topic(state.metadata, state.brokers, produce_request.topic, produce_request.partition), state}
      broker -> {broker, state}
    end

    response = case broker do
      nil    ->
        :ok = Logger.log(:error, "Leader for topic #{produce_request.topic} is not available")
        :leader_not_available
      broker -> case produce_request.required_acks do
        0 ->  KafkaEx.NetworkClient.send_async_request(broker, produce_request_data)
        _ -> KafkaEx.NetworkClient.send_sync_request(broker, produce_request_data, state.sync_timeout) |> Proto.Produce.parse_response
      end
    end

    state = %{state | correlation_id: correlation_id + 1}
    {:reply, response, state}
  end

  def handle_call({:fetch, topic, partition, offset, wait_time, min_bytes, max_bytes, auto_commit}, _from, state) do
    {response, state} = fetch(topic, partition, offset, wait_time, min_bytes, max_bytes, state, auto_commit)

    {:reply, response, state}
  end

  def handle_call({:offset, topic, partition, time}, _from, state) do
    offset_request = Proto.Offset.create_request(state.correlation_id, client_id, topic, partition, time)
    {broker, state} = case Proto.Metadata.Response.broker_for_topic(state.metadata, state.brokers, topic, partition) do
      nil    ->
        state = update_metadata(state)
        {Proto.Metadata.Response.broker_for_topic(state.metadata, state.brokers, topic, partition), state}
      broker -> {broker, state}
    end

    {response, state} = case broker do
      nil ->
        :ok = Logger.log(:error, "Leader for topic #{topic} is not available")
        {:topic_not_found, state}
      _ ->
        response = broker
         |> KafkaEx.NetworkClient.send_sync_request(offset_request, state.sync_timeout)
         |> Proto.Offset.parse_response
        state = %{state | correlation_id: state.correlation_id + 1}
        {response, state}
    end

    {:reply, response, state}
  end

  def handle_call({:metadata, topic}, _from, state) do
    {correlation_id, metadata} = retrieve_metadata(state.brokers, state.correlation_id, state.sync_timeout, topic)
    state = %{state | metadata: metadata, correlation_id: correlation_id}
    {:reply, metadata, state}
  end

  def handle_call({:create_stream, handler, handler_init}, _from, state) do
    if state.event_pid && Process.alive?(state.event_pid) do
      info = Process.info(self)
      :ok = Logger.log(:warn, "'#{info[:registered_name]}' already streaming handler '#{handler}'")
    else
      {:ok, event_pid}  = GenEvent.start_link
      state = %{state | event_pid: event_pid}
      :ok = GenEvent.add_handler(state.event_pid, handler, handler_init)
    end
    {:reply, GenEvent.stream(state.event_pid), state}
  end

  def handle_call({:offset_fetch, _}, _from, _state), do: raise "Offset Fetch is not supported in 0.8.0 version of kafka"
  def handle_call({:offset_commit, _}, _from, _state), do: raise "Offset Commit is not supported in 0.8.0 version of kafka"
  def handle_call(:consumer_group, _from, _state), do: raise "Consumer Group is not supported in 0.8.0 version of kafka"
  def handle_call({:consumer_group_metadata, _}, _from, _state), do: raise "Consumer Group Metadata is not supported in 0.8.0 version of kafka"
  def handle_call({:join_group, _, _}, _from, _state), do: raise "Join Group is not supported in 0.8.0 version of kafka"
  def handle_call({:sync_group, _, _, _, _}, _from, _state), do: raise "Sync Group is not supported in 0.8.0 version of kafka"
  def handle_call({:heartbeat, _, _, _}, _from, _state), do: raise "Heartbeat is not supported in 0.8.0 version of kafka"

  @wait_time 10
  @min_bytes 1
  @max_bytes 1_000_000
  def handle_info({:start_streaming, _topic, _partition, _offset, _handler, _auto_commit},
                  state = %State{event_pid: nil}) do
    # our streaming could have been canceled with a streaming update in-flight
    {:noreply, state}
  end
  def handle_info({:start_streaming, topic, partition, offset, handler, auto_commit}, state) do
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

  def handle_info(:stop_streaming, state) do
    :ok = Logger.log(:debug, "Stopped worker #{inspect state.worker_name} from streaming")
    GenEvent.stop(state.event_pid)
    {:noreply, %{state | event_pid: nil}}
  end

  def handle_info(:update_metadata, state) do
    {:noreply, update_metadata(state)}
  end

  def handle_info(_, state) do
    {:noreply, state}
  end

  def terminate(_, state) do
    :ok = Logger.log(:debug, "Shutting down worker #{inspect state.worker_name}")
    if state.event_pid do
      GenEvent.stop(state.event_pid)
    end
    Enum.each(state.brokers, fn(broker) -> KafkaEx.NetworkClient.close_socket(broker.socket) end)
  end

  defp fetch(topic, partition, offset, wait_time, min_bytes, max_bytes, state, _auto_commit) do
    fetch_request = Proto.Fetch.create_request(state.correlation_id, client_id, topic, partition, offset, wait_time, min_bytes, max_bytes)
    {broker, state} = case Proto.Metadata.Response.broker_for_topic(state.metadata, state.brokers, topic, partition) do
      nil    ->
        state = update_metadata(state)
        {Proto.Metadata.Response.broker_for_topic(state.metadata, state.brokers, topic, partition), state}
      broker -> {broker, state}
    end

    case broker do
      nil ->
        :ok = Logger.log(:error, "Leader for topic #{topic} is not available")
        {:topic_not_found, state}
      _ ->
        response = broker
          |> KafkaEx.NetworkClient.send_sync_request(fetch_request, state.sync_timeout)
          |> Proto.Fetch.parse_response
        state = %{state | correlation_id: state.correlation_id + 1}
        {response, state}
    end
  end
end
