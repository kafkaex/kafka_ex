defmodule KafkaEx.Server do
  @client_id "kafka_ex"

  ### GenServer Callbacks
  use GenServer

  def start_link(uris, name \\ __MODULE__) do
    GenServer.start_link(__MODULE__, uris, [name: name])
  end

  def init(uris) do
    socket_map = KafkaEx.Connection.connect_brokers(uris)
    send(self, :metadata)
    {:ok, {0, %{}, socket_map, nil}}
  end

  def handle_call({:produce, topic, partition, value, key, required_acks, timeout}, _from, {correlation_id, _metadata, socket_map, event_pid} = state) do
    data = KafkaEx.Protocol.Produce.create_request(correlation_id, @client_id, topic, partition, value, key, required_acks, timeout)
    metadata = topic_metadata(state, topic)
    socket = get_socket_for_broker(socket_map, metadata, topic, partition)
    send_data(socket, data)
    parsed_response = case required_acks do
      0 -> :ok
      _ -> handle_produce_response(timeout)
    end
    {:reply, parsed_response, {correlation_id + 1, metadata, socket_map, event_pid}}
  end

  def handle_call({:fetch, topic, partition, offset, wait_time, min_bytes, max_bytes}, _from, {correlation_id, _metadata, socket_map, event_pid} = state) do
    data = KafkaEx.Protocol.Fetch.create_request(correlation_id, @client_id, topic, partition, offset, wait_time, min_bytes, max_bytes)
    metadata = topic_metadata(state, topic)
    socket = get_socket_for_broker(socket_map, metadata, topic, partition)
    send_data(socket, data)
    {:reply, handle_fetch_response, {correlation_id + 1, metadata, socket_map, event_pid}}
  end

  def handle_call({:offset, topic, partition, time}, _from, {correlation_id, _metadata, socket_map, event_pid} = state) do
    data = KafkaEx.Protocol.Offset.create_request(correlation_id, @client_id, topic, partition, time)
    metadata = topic_metadata(state, topic)
    socket = get_socket_for_broker(socket_map, metadata, topic, partition)
    send_data(socket, data)
    {:reply, handle_offset_response, {correlation_id + 1, metadata, socket_map, event_pid}}
  end

  def handle_call({:metadata, topic}, _from, {correlation_id, metadata, socket_map, event_pid} = state) do
    data = topic_metadata(state, topic)
    {:reply, data, {correlation_id + 1, metadata, socket_map, event_pid}}
  end

  @wait_time 10
  @min_bytes 1
  @max_bytes 1_000_000
  def handle_info({:start_streaming, topic, partition, offset, pid, handler}, {correlation_id, _metadata, socket_map, _event_pid} = state) do
    data = KafkaEx.Protocol.Fetch.create_request(correlation_id, @client_id, topic, partition, offset, @wait_time, @min_bytes, @max_bytes)
    metadata = topic_metadata(state, topic)
    socket = get_socket_for_broker(socket_map, metadata, topic, partition)
    send_data(socket, data)
    start_stream(pid, handler, topic, partition)
    {:noreply, state}
  end

  def handle_info(:metadata, {correlation_id, _, socket_map, event_pid} = state) do
    {:noreply, {correlation_id + 1, topic_metadata(state, ""), socket_map, event_pid}}
  end

  def handle_info({:update_metadata, new_correlation_id, metadata, topic_metadata}, {_correlation_id, _, socket_map, event_pid}) do
    updated_metadata = Map.merge(metadata, topic_metadata, fn(_, v1, v2) -> Map.merge(v1, v2) end)
    {:noreply, {new_correlation_id, updated_metadata, socket_map, event_pid}}
  end

  def handle_info(_, state) do
    {:noreply, state}
  end

  def terminate(_, {_, _, socket_map, nil}) do
    Map.values(socket_map) |> Enum.each(&KafkaEx.Connection.close/1)
  end

  def terminate(_, {_, _, socket_map, event_pid}) do
    Process.exit(event_pid, :kill)
    Map.values(socket_map) |> Enum.each(&KafkaEx.Connection.close/1)
  end

  @success                    0
  @retry_count                3
  @retry_delay                500
  @leader_not_available       5
  defp topic_metadata({correlation_id, metadata, socket_map, event_pid}, topic, retry_count \\ @retry_count) do
    data = KafkaEx.Protocol.Metadata.create_request(correlation_id + 1, @client_id, topic)
    Map.values(socket_map) |> send_metadata_request(data)
    metadata_response = handle_metadata_response

    case byte_size(topic) do
      @success -> metadata_response
      _ -> cond do
        metadata_response[:topics][topic][:error_code] == @success and retry_count != @success ->
          send(self, {:update_metadata, correlation_id, metadata, metadata_response})
          metadata_response
        @leader_not_available == metadata_response[:topics][topic][:error_code] and retry_count != @success ->
          :timer.sleep(@retry_delay)
          topic_metadata({correlation_id, metadata, socket_map, event_pid}, topic, retry_count - 2)
        true -> raise "got #{metadata_response[:topics][topic][:error_code]}"
      end
    end
  end

  defp send_metadata_request(rest, _) when length(rest) == 0 do
    raise "Cannot send metadata request"
  end

  defp send_metadata_request([socket|rest], data) do
    try do
      send_data(socket, data)
    rescue
      _ in KafkaEx.ConnectionError -> send_metadata_request(rest, data)
    end
  end

  defp get_socket_for_broker(socket_map, metadata, topic, partition) do
    brokers = Map.get(metadata, :brokers, %{})
    leader = Map.get(metadata, :topics, %{}) |> Map.get(topic, %{}) |> Map.get(:partitions, %{}) |> Map.get(partition, %{}) |> Map.get(:leader) #handle when there is no leader or no partition
    socket_map[brokers[leader]]
  end

  defp send_data(socket, data) do
    case KafkaEx.Connection.send(data, socket) do
      {:error, reason} -> raise KafkaEx.ConnectionError, message: "Cannot send data to broker due to #{inspect reason}"
      response -> response
    end
  end

  defp handle_produce_response(timeout) do
    receive do
      {:tcp, _, data} -> KafkaEx.Protocol.Produce.parse_response(data)
    after
      (timeout + 1000) -> :timeout
    end
  end

  defp start_stream(pid, handler, topic, partition) do
    receive do
      {:tcp, _, data} ->
        if byte_size(data) > 0 do
          {:ok, map} = KafkaEx.Protocol.Fetch.parse_response(data)
          map[topic][partition][:message_set] |>
            Enum.each(fn(message_set) -> GenEvent.notify(pid, message_set) end)
        end
        start_stream(pid, handler, topic, partition)
    end
  end

  defp handle_fetch_response do
    receive do
      {:tcp, _, data} -> KafkaEx.Protocol.Fetch.parse_response(data)
    end
  end

  defp handle_offset_response do
    receive do
      {:tcp, _, data} -> KafkaEx.Protocol.Offset.parse_response(data)
    end
  end

  defp handle_metadata_response do
    receive do
      {:tcp, _, data} -> KafkaEx.Protocol.Metadata.parse_response(data)
    after
      5000 -> :timeout
    end
  end
end
