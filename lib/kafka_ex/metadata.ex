defmodule KafkaEx.Metadata do

  @default_update_threshold 5 * 60 * 60 * 1000

  def new(host_ports) do
    %{topics: %{}, brokers: %{}, timestamp: 0, bootstrap: host_ports, update_threshold: @default_update_threshold}
  end

  def add_topic(metadata, network_client, topic) do
    if Enum.member?(Map.keys(metadata.topics), topic) && metadata.topics[topic].error_code == :no_error do
      {metadata, network_client}
    else
      update_metadata(metadata, network_client, [topic])
    end
  end

  def broker_for_topic(metadata, topic, partition \\ 0) do
    broker = nil
    topic_info = metadata.topics[topic]
    if topic_info && topic_info.partitions[partition] do
      leader = topic_info.partitions[partition].leader
      broker = metadata.brokers[leader]
    end
    broker
  end

  def set_update_threshold(metadata, value) when is_integer(value) do
    %{metadata | update_threshold: value}
  end

  def set_update_threshold(_metadata, _value) do
    raise "Update threshold value must be an integer number of milliseconds"
  end

  def update(metadata, network_client, force \\ false) do
    if force || KafkaEx.Util.current_timestamp - metadata.timestamp >= metadata.update_threshold do
      update_metadata(metadata, network_client, topics(metadata))
    else
      {metadata, network_client}
    end
  end

  defp topics(metadata) do
    Map.keys(metadata.topics)
  end

  defp broker_list(metadata) do
    if metadata[:bootstrap] do
      metadata.bootstrap
    else
      Map.values(metadata.brokers)
    end
  end

  defp has_leader_not_available?(metadata) do
    Enum.any?(Enum.map(metadata.topics, fn({topic, values}) -> values.error_code end), fn(error) -> error == :leader_not_available end)
  end

  # Note: need to check for :leader_not_available for the topics, and wait until error clears to return
  @retry_count 3
  def update_metadata(metadata, client, topic_list, retry_count \\ @retry_count) do
    request_fn = KafkaEx.Protocol.Metadata.create_request_fn(topic_list)
    case KafkaEx.NetworkClient.send_request(client, broker_list(metadata), request_fn) do
      {:error, reason} -> {:error, reason}
      {client, response} ->
        from_broker = KafkaEx.Protocol.Metadata.parse_response(response)
        if has_leader_not_available?(from_broker) && retry_count > 0 do
          :timer.sleep(100)
          update_metadata(metadata, client, topic_list, retry_count - 1)
        else
          updated_metadata = Map.merge(metadata, from_broker) |> Map.put(:timestamp, KafkaEx.Util.current_timestamp)
          updated_client = KafkaEx.NetworkClient.update_from_metadata(client, Map.values(updated_metadata.brokers))
          {updated_metadata, updated_client}
        end
    end
  end
end
