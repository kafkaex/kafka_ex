defmodule KafkaEx.Server do
  @client_id "kafka_ex"

  ### GenServer Callbacks
  use GenServer

  def start_link(uris, name \\ __MODULE__) do
    GenServer.start_link(__MODULE__, uris, [name: name])
  end

  def init(uris) do
    client = KafkaEx.NetworkClient.new(@client_id)
    metadata = KafkaEx.Metadata.new(uris)
    send(self, :metadata)
    {:ok, {metadata, client, nil}}
  end

  defp send_request(metadata, client, topic, partition, request_fn, parser, timeout \\ 100) do
    {metadata, client} = KafkaEx.Metadata.add_topic(metadata, client, topic)
    broker = KafkaEx.Metadata.broker_for_topic(metadata, topic, partition)
    {client, response} = KafkaEx.NetworkClient.send_request(client, [broker], request_fn, timeout)
    if parser do
      {metadata, client, parser.(response)}
    else
      {metadata, client, response}
    end
  end

  def handle_call({:produce, topic, partition, value, key, required_acks, timeout}, _from, {metadata, client, event_pid}) do
    request_fn = KafkaEx.Protocol.Produce.create_request_fn(topic, partition, value, key, required_acks, timeout)
    {metadata, client, response} = send_request(metadata, client, topic, partition, request_fn, nil, timeout)
    parsed_response = case required_acks do
      0 -> :ok
      _ -> KafkaEx.Protocol.Produce.parse_response(response)
    end
    {:reply, parsed_response, {metadata, client, event_pid}}
  end

  def handle_call({:fetch, topic, partition, offset, wait_time, min_bytes, max_bytes}, _from, {metadata, client, event_pid}) do
    request_fn = KafkaEx.Protocol.Fetch.create_request_fn(topic, partition, offset, wait_time, min_bytes, max_bytes)
    {metadata, client, response} = send_request(metadata, client, topic, partition, request_fn, &KafkaEx.Protocol.Fetch.parse_response/1)
    {:reply, response, {metadata, client, event_pid}}
  end

  def handle_call({:offset, topic, partition, time}, _from, {metadata, client, event_pid}) do
    request_fn = KafkaEx.Protocol.Offset.create_request_fn(topic, partition, time)
    {metadata, client, response} = send_request(metadata, client, topic, partition, request_fn, &KafkaEx.Protocol.Offset.parse_response/1)
    {:reply, response, {metadata, client, event_pid}}
  end

  def handle_call({:metadata, topic}, _from, {metadata, client, event_pid}) do
    {metadata, client} = KafkaEx.Metadata.add_topic(metadata, client, topic)
    {:reply, metadata, {metadata, client, event_pid}}
  end

  @wait_time 10
  @min_bytes 1
  @max_bytes 1_000_000
  @sleep_when_empty 1000
  def handle_info({:stream, topic, partition, offset, pid, handler, sleep}, {metadata, client, event_pid}) do
    if sleep > 0, do: :timer.sleep(sleep)

    request_fn = KafkaEx.Protocol.Fetch.create_request_fn(topic, partition, offset, @wait_time, @min_bytes, @max_bytes)
    {metadata, client, {:ok, response}} = send_request(metadata, client, topic, partition, request_fn, &KafkaEx.Protocol.Fetch.parse_response/1)
    message_sets = response[topic][partition][:message_set]

    unless Enum.empty?(message_sets) do
      Enum.each(message_sets, fn(message_set) -> GenEvent.notify(pid, message_set) end)
      last_offset = Enum.at(Enum.reverse(message_sets), 0).offset
      send(self, {:stream, topic, partition, last_offset+1, pid, handler, 0})
    else
      send(self, {:stream, topic, partition, offset, pid, handler, @sleep_when_empty})
    end

    {:noreply, {metadata, client, event_pid}}
  end

  def handle_info(:metadata, {metadata, client, event_pid}) do
    {metadata, client} = KafkaEx.Metadata.update(metadata, client)
    {:noreply, {metadata, client, event_pid}}
  end

  def handle_info({:update_metadata, new_correlation_id, metadata, topic_metadata}, {_correlation_id, _, socket_map, event_pid}) do
    updated_metadata = Map.merge(metadata, topic_metadata, fn(_, v1, v2) -> Map.merge(v1, v2) end)
    {:noreply, {new_correlation_id, updated_metadata, socket_map, event_pid}}
  end

  def handle_info(_, state) do
    {:noreply, state}
  end

  def terminate(_, {_, client, nil}) do
    KafkaEx.NetworkClient.shutdown(client)
  end

  def terminate(_, {_, client, event_pid}) do
    KafkaEx.NetworkClient.shutdown(client)
    Process.exit(event_pid, :kill)
  end
end
