defmodule KafkaEx.Server do
  alias KafkaEx.Protocol, as: Proto
  @client_id "kafka_ex"
  @consumer_group "kafka_ex"
  defstruct metadata: %Proto.Metadata.Response{}, brokers: [], event_pid: nil, consumer_metadata: %Proto.ConsumerMetadata.Response{}, correlation_id: 0, consumer_group: @client_id

  ### GenServer Callbacks
  use GenServer

  def start_link(args, name \\ __MODULE__)

  def start_link(args, name) do
    GenServer.start_link(__MODULE__, args, [name: name])
  end

  def init(args) do
    uris = Keyword.get(args, :uris, [])
    consumer_group = Keyword.get(args, :consumer_group, @consumer_group)
    brokers = Enum.map(uris, fn({host, port}) -> %Proto.Metadata.Broker{host: host, port: port, socket: KafkaEx.NetworkClient.create_socket(host, port)} end)
    {correlation_id, metadata} = metadata(brokers, 0)
    {:ok, _} = :timer.send_interval(30000, :update_metadata)
    {:ok, _} = :timer.send_interval(30000, :update_consumer_metadata)
    {:ok, %__MODULE__{metadata: metadata, brokers: brokers, correlation_id: correlation_id, consumer_group: consumer_group}}
  end

  def handle_call({:produce, topic, partition, value, key, required_acks, timeout}, _from, state) do
    correlation_id = state.correlation_id + 1
    produce_request = Proto.Produce.create_request(correlation_id, @client_id, topic, partition, value, key, required_acks, timeout)
    {broker, state} = case Proto.Metadata.Response.broker_for_topic(state.metadata, state.brokers, topic, partition) do
      nil    ->
        {correlation_id, _} = metadata(state.brokers, state.correlation_id, topic)
        state = %{update_metadata(state) | correlation_id: correlation_id}
        {Proto.Metadata.Response.broker_for_topic(state.metadata, state.brokers, topic, partition), state}
      broker -> {broker, state}
    end

    response = case broker do
      nil    -> :leader_not_available
      broker -> case required_acks do
        0 ->  KafkaEx.NetworkClient.send_async_request(broker, produce_request)
        _ -> KafkaEx.NetworkClient.send_sync_request(broker, produce_request) |> Proto.Produce.parse_response
      end
    end

    state = %{state | correlation_id: correlation_id+1}
    {:reply, response, state}
  end

  def handle_call({:fetch, topic, partition, offset, wait_time, min_bytes, max_bytes}, _from, state) do
    {response, state} = fetch(topic, partition, offset, wait_time, min_bytes, max_bytes, state)

    {:reply, response, state}
  end

  def handle_call({:offset, topic, partition, time}, _from, state) do
    offset_request = Proto.Offset.create_request(state.correlation_id, @client_id, topic, partition, time)
    {broker, state} = case Proto.Metadata.Response.broker_for_topic(state.metadata, state.brokers, topic, partition) do
      nil    ->
        state = update_metadata(state)
        {Proto.Metadata.Response.broker_for_topic(state.metadata, state.brokers, topic, partition), state}
      broker -> {broker, state}
    end

    {response, state} = case broker do
      nil -> {:topic_not_found, state}
      _ ->
        response = KafkaEx.NetworkClient.send_sync_request(broker, offset_request) |> Proto.Offset.parse_response
        state = %{state | correlation_id: state.correlation_id+1}
        {response, state}
    end

    {:reply, response, state}
  end

  def handle_call({:offset_fetch, offset_fetch}, _from, state) do
    {broker, state} = case Proto.ConsumerMetadata.Response.broker_for_consumer_group(state.brokers, state.consumer_metadata) do
      nil -> {_, state} = update_consumer_metadata(state, offset_fetch.consumer_group)
        {Proto.ConsumerMetadata.Response.broker_for_consumer_group(state.brokers, state.consumer_metadata), state}
      broker -> {broker, state}
    end

    offset_fetch_request = Proto.OffsetFetch.create_request(state.correlation_id, @client_id, offset_fetch)

    {response, state} = case broker do
      nil -> {:topic_not_found, state}
      _ ->
        response = KafkaEx.NetworkClient.send_sync_request(broker, offset_fetch_request) |> Proto.OffsetFetch.parse_response
        state = %{state | correlation_id: state.correlation_id+1}
        {response, state}
    end

    {:reply, response, state}
  end

  def handle_call({:offset_commit, offset_commit}, _from, state) do
    {broker, state} = case Proto.ConsumerMetadata.Response.broker_for_consumer_group(state.brokers, state.consumer_metadata) do
      nil -> {_, state} = update_consumer_metadata(state, offset_commit.consumer_group)
        {Proto.ConsumerMetadata.Response.broker_for_consumer_group(state.brokers, state.consumer_metadata) || hd(state.brokers), state}
      broker -> {broker, state}
    end

    offset_commit_request = Proto.OffsetCommit.create_request(state.correlation_id, @client_id, offset_commit)
    response = KafkaEx.NetworkClient.send_sync_request(broker, offset_commit_request) |> Proto.OffsetCommit.parse_response

    {:reply, response, %{state | correlation_id: state.correlation_id+1}}
  end

  def handle_call({:consumer_group_metadata, consumer_group}, _from, state) do
    {consumer_metadata, state} = update_consumer_metadata(state, consumer_group)
    {:reply, consumer_metadata, state}
  end

  def handle_call({:metadata, topic}, _from, state) do
    {correlation_id, metadata} = metadata(state.brokers, state.correlation_id, topic)
    state = %{state | metadata: metadata, correlation_id: correlation_id}
    {:reply, metadata, state}
  end

  def handle_call({:create_stream, handler}, _from, state) do
    state = unless state.event_pid && Process.alive?(state.event_pid) do
      {:ok, event_pid}  = GenEvent.start_link
      %{state | event_pid: event_pid}
    end
    :ok = GenEvent.add_handler(state.event_pid, handler, [])
    {:reply, GenEvent.stream(state.event_pid), state}
  end

  @wait_time 10
  @min_bytes 1
  @max_bytes 1_000_000
  def handle_info({:start_streaming, topic, partition, offset, handler}, state) do
    {response, state} = fetch(topic, partition, offset, @wait_time, @min_bytes, @max_bytes, state)
    offset = case response do
      :topic_not_found -> offset
      _ -> offset = response |> hd |> Map.get(:partitions) |> hd |> Map.get(:message_set) |> Enum.reduce(offset, fn(message_set, _) ->
        GenEvent.notify(state.event_pid, message_set)
        message_set.offset
      end)
      offset + 1
    end
    :timer.sleep(500)
    send(self, {:start_streaming, topic, partition, offset, handler})
    {:noreply, state}
  end

  def handle_info(:stop_streaming, state) do
    GenEvent.stop(state.event_pid)
    {:noreply, %{state | event_pid: nil}}
  end

  def handle_info(:update_metadata, state) do
    {:noreply, update_metadata(state)}
  end

  def handle_info(:update_consumer_metadata, state) do
    {:noreply, update_consumer_metadata(state, state.consumer_group) |> elem(1)}
  end

  def handle_info(_, state) do
    {:noreply, state}
  end

  def terminate(_, state) do
    GenEvent.stop(state.event_pid)
    Enum.each(state.brokers, &KafkaEx.NetworkClient.close_socket/1)
  end

  defp update_consumer_metadata(state, consumer_group), do: update_consumer_metadata(state, consumer_group, 3)

  defp update_consumer_metadata(state, _, 0), do: {%Proto.ConsumerMetadata.Response{}, state}

  defp update_consumer_metadata(state, consumer_group, retry) do
    consumer_group_metadata_request = Proto.ConsumerMetadata.create_request(state.correlation_id, @client_id, consumer_group)
    data = Enum.find_value(state.brokers, fn(broker) -> KafkaEx.NetworkClient.send_sync_request(broker, consumer_group_metadata_request) end)
    response = Proto.ConsumerMetadata.parse_response(data)
    case response.error_code do
      0 -> {response, %{state | consumer_metadata: response, consumer_group: consumer_group, correlation_id: state.correlation_id + 1}}
      _ -> :timer.sleep(300)
        update_consumer_metadata(%{state | consumer_group: consumer_group, correlation_id: state.correlation_id + 1}, consumer_group, retry-1)
    end
  end

  defp update_metadata(state) do
    {correlation_id, metadata} = metadata(state.brokers, state.correlation_id)
    metadata_brokers = metadata.brokers
    brokers = remove_stale_brokers(state.brokers, metadata_brokers) |> add_new_brokers(metadata_brokers)
    %{state | metadata: metadata, brokers: brokers, correlation_id: correlation_id+1}
  end

  defp remove_stale_brokers(brokers, metadata_brokers) do
    {brokers_to_keep, brokers_to_remove} = Enum.partition(brokers, fn(broker) ->
      Enum.find_value(metadata_brokers, &(broker.host == &1.host && broker.port == &1.port))
    end)
    Enum.each(brokers_to_remove, &KafkaEx.NetworkClient.close_socket/1)
    brokers_to_keep
  end

  defp add_new_brokers(brokers, []), do: brokers

  defp add_new_brokers(brokers, [metadata_broker|metadata_brokers]) do
    case Enum.find(brokers, &(metadata_broker.host == &1.host && metadata_broker.port == &1.port)) do
      nil -> add_new_brokers([%{metadata_broker | socket: KafkaEx.NetworkClient.create_socket(metadata_broker.host, metadata_broker.port)} | brokers], metadata_brokers)
      _ -> add_new_brokers(brokers, metadata_brokers)
    end
  end

  @retry_count 3
  defp metadata(brokers, correlation_id, topic \\ []), do: metadata(brokers, correlation_id, topic, @retry_count)

  defp metadata(_, correlation_id, _, 0), do: {correlation_id, %Proto.Metadata.Response{}}

  defp metadata(brokers, correlation_id, topic, retry) do
    metadata_request = Proto.Metadata.create_request(correlation_id, @client_id, topic)
    data = Enum.find_value(brokers, fn(broker) -> KafkaEx.NetworkClient.send_sync_request(broker, metadata_request) end)
    response = Proto.Metadata.parse_response(data)

    case Enum.any?(response.topic_metadatas, &(&1.error_code == 5)) do
      true ->
        :timer.sleep(300)
        metadata(brokers, correlation_id+1, topic, retry-1)
      _ -> {correlation_id+1, response}
    end
  end

  defp fetch(topic, partition, offset, wait_time, min_bytes, max_bytes, state) do
    fetch_request = Proto.Fetch.create_request(state.correlation_id, @client_id, topic, partition, offset, wait_time, min_bytes, max_bytes)
    {broker, state} = case Proto.Metadata.Response.broker_for_topic(state.metadata, state.brokers, topic, partition) do
      nil    ->
        state = update_metadata(state)
        {Proto.Metadata.Response.broker_for_topic(state.metadata, state.brokers, topic, partition), state}
      broker -> {broker, state}
    end

    case broker do
      nil -> {:topic_not_found, state}
      _ ->
        response = KafkaEx.NetworkClient.send_sync_request(broker, fetch_request) |> Proto.Fetch.parse_response
        state = %{state | correlation_id: state.correlation_id+1}
        {response, state}
    end
  end

end
