defmodule KafkaEx.Server do
  @client_id "kafka_ex"
  defstruct metadata: %{}, client: %{}, event_pid: nil, consumer_metadata: %KafkaEx.Protocol.ConsumerMetadata.Response{}

  ### GenServer Callbacks
  use GenServer

  def start_link(uris, name \\ __MODULE__) do
    GenServer.start_link(__MODULE__, uris, [name: name])
  end

  def init(uris) do
    client = KafkaEx.NetworkClient.new(@client_id)
    metadata = KafkaEx.Metadata.new(uris)
    send(self, :metadata)
    {:ok, %KafkaEx.Server{metadata: metadata, client: client}}
  end

  def handle_call({:produce, topic, partition, value, key, required_acks, timeout}, _from, state) do
    data = [topic, partition, value, key, required_acks, timeout]
    {metadata, client, response} = send_request(state.metadata, state.client, topic, partition, KafkaEx.Protocol.Produce, data, timeout)
    state = %{state | :metadata => metadata, :client => client}
    response = case required_acks do
      0 -> :ok
      _ -> response
    end
    {:reply, response, state}
  end

  def handle_call({:fetch, topic, partition, offset, wait_time, min_bytes, max_bytes}, _from, state) do
    data = [topic, partition, offset, wait_time, min_bytes, max_bytes]
    {metadata, client, response} = send_request(state.metadata, state.client, topic, partition, KafkaEx.Protocol.Fetch, data)
    state = %{state | :metadata => metadata, :client => client}
    {:reply, response, state}
  end

  def handle_call({:offset, topic, partition, time}, _from, state) do
    data = [topic, partition, time]
    {metadata, client, response} = send_request(state.metadata, state.client, topic, partition, KafkaEx.Protocol.Offset, data)
    state = %{state | :metadata => metadata, :client => client}
    {:reply, response, state}
  end

  def handle_call({:offset_fetch, offset_fetch_request}, _from, state) do
    if state.consumer_metadata.coordinator_host == "" do
      consumer_metadata = update_consumer_metadata(state, offset_fetch_request.consumer_group)
      state = %{state | :consumer_metadata => consumer_metadata}
    end
    client = KafkaEx.NetworkClient.send_request(state.client, [{state.consumer_metadata.coordinator_host, state.consumer_metadata.coordinator_port}], [offset_fetch_request], KafkaEx.Protocol.OffsetFetch)
    {_, data} = KafkaEx.NetworkClient.get_response(client)
    response = KafkaEx.Protocol.OffsetFetch.parse_response(data)
    {:reply, response, state}
  end

  def handle_call({:offset_commit, offset_commit_request}, _from, state) do
    if state.consumer_metadata.coordinator_host == "" do
      consumer_metadata = update_consumer_metadata(state, offset_commit_request.consumer_group)
      state = %{state | :consumer_metadata => consumer_metadata}
    end
    client = KafkaEx.NetworkClient.send_request(state.client, [{state.consumer_metadata.coordinator_host, state.consumer_metadata.coordinator_port}], [offset_commit_request], KafkaEx.Protocol.OffsetCommit)
    {_, data} = KafkaEx.NetworkClient.get_response(client)
    response = KafkaEx.Protocol.OffsetCommit.parse_response(data)
    {:reply, response, state}
  end

  def handle_call({:consumer_group_metadata, consumer_group}, _from, state) do
    consumer_metadata = update_consumer_metadata(state, consumer_group)
    {:reply, consumer_metadata, %{state | :consumer_metadata => consumer_metadata}}
  end

  def handle_call({:metadata, topic}, _from, state) do
    {metadata, client} = KafkaEx.Metadata.add_topic(state.metadata, state.client, topic)
    state = %{state | :metadata => metadata, :client => client}
    {:reply, metadata, state}
  end

  def handle_call({:create_stream, handler}, _from, state) do
    unless state.event_pid && Process.alive?(state.event_pid) do
      {:ok, event_pid}  = GenEvent.start_link
      state = %{state | event_pid: event_pid}
    end
    GenEvent.add_handler(state.event_pid, handler, [])
    {:reply, GenEvent.stream(state.event_pid), state}
  end

  defp update_consumer_metadata(state, consumer_group) do
    consumer_client = KafkaEx.NetworkClient.send_request(state.client, Map.keys(state.client.hosts), [consumer_group], KafkaEx.Protocol.ConsumerMetadata)
    {_, data} = KafkaEx.NetworkClient.get_response(consumer_client)
    KafkaEx.Protocol.ConsumerMetadata.parse_response(data)
  end

  @wait_time 10
  @min_bytes 1
  @max_bytes 1_000_000
  def handle_info({:start_streaming, topic, partition, offset, handler}, state) do
    {metadata, client} = start_stream(state.event_pid, state.metadata, state.client, handler, topic, partition, offset)
    state = %{state | :metadata => metadata, :client => client}
    {:noreply, state}
  end

  def handle_info(:metadata, state) do
    {metadata, client} = KafkaEx.Metadata.update(state.metadata, state.client)
    state = %{state | :metadata => metadata, :client => client}
    {:noreply, state}
  end

  def handle_info(_, state) do
    {:noreply, state}
  end

  def terminate(_, state) do
    KafkaEx.NetworkClient.shutdown(state.client)
  end

  defp notify_handler([], _handler_pid, offset), do: offset

  defp notify_handler([message_set|message_sets], handler_pid, _offset) do
    GenEvent.notify(handler_pid, message_set)
    notify_handler(message_sets, handler_pid, message_set.offset)
  end

  defp handle_message_sets([], pid, metadata, client, handler, topic, partition, offset) do
    :timer.sleep(1000)
    start_stream(pid, metadata, client, handler, topic, partition, offset)
  end

  defp handle_message_sets(message_sets, pid, metadata, client, handler, topic, partition, offset) do
    last_offset = notify_handler(message_sets, pid, offset)
    start_stream(pid, metadata, client, handler, topic, partition, last_offset+1)
  end

  defp start_stream(pid, metadata, client, handler, topic, partition, offset) do
    data = [topic, partition, offset, @wait_time, @min_bytes, @max_bytes]
    {metadata, client} = send_request(metadata, client, topic, partition, KafkaEx.Protocol.Fetch, data, 100, false)
    receive do
      {:tcp, _, data} ->
        {:ok, response} = KafkaEx.Protocol.Fetch.parse_response(data)
        message_sets = response[topic][partition].message_set
        handle_message_sets(message_sets, pid, metadata, client, handler, topic, partition, offset)
      :stop_streaming ->
        {metadata, client}
    end
  end

  defp send_request(metadata, client, topic, partition, protocol_mod, data, timeout \\ 100, require_response \\ true) do
    {metadata, client} = KafkaEx.Metadata.add_topic(metadata, client, topic)

    broker = KafkaEx.Metadata.broker_for_topic(metadata, topic, partition)
    if broker == nil, do: raise "No broker found for topic #{topic}"

    client = KafkaEx.NetworkClient.send_request(client, [broker], data, protocol_mod)
    if require_response do
      {client, response} = KafkaEx.NetworkClient.get_response(client, timeout)
      {metadata, client, protocol_mod.parse_response(response)}
    else
      {metadata, client}
    end
  end
end
