defmodule KafkaEx.ServerBase do
  defmacro __using__(_) do
    quote do
      import KafkaEx.ServerBase.Common
      alias KafkaEx.Protocol, as: Proto
      alias KafkaEx.ServerBase.Common.State
      require Logger
      use GenServer
    end
  end

  defmodule Common do
    require Logger
    alias KafkaEx.Protocol, as: Proto

    @client_id   "kafka_ex"
    @retry_count 3

    defmodule State do
      defstruct(metadata: %Proto.Metadata.Response{},
      brokers: [],
      event_pid: nil,
      consumer_metadata: %Proto.ConsumerMetadata.Response{},
      correlation_id: 0,
      consumer_group: nil,
      metadata_update_interval: nil,
      consumer_group_update_interval: nil,
      worker_name: KafkaEx.Server,
      sync_timeout: nil)
    end

    def client_id, do: @client_id
    def retry_count, do: @retry_count

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
      {correlation_id, %Proto.Metadata.Response{}}
    end
    def retrieve_metadata(brokers, correlation_id, sync_timeout, topic, retry, _error_code) do
      metadata_request = Proto.Metadata.create_request(correlation_id, @client_id, topic)
      data = first_broker_response(metadata_request, brokers, sync_timeout)
      response = case data do
                   nil ->
                     Logger.log(:error, "Unable to fetch metadata from any brokers.  Timeout is #{sync_timeout}.")
                     raise "Unable to fetch metadata from any brokers.  Timeout is #{sync_timeout}."
                     :no_metadata_available
                   data ->
                     Proto.Metadata.parse_response(data)
                 end

                 case Enum.find(response.topic_metadatas, &(&1.error_code == :leader_not_available)) do
        nil  -> {correlation_id + 1, response}
        topic_metadata ->
          :timer.sleep(300)
          retrieve_metadata(brokers, correlation_id + 1, sync_timeout, topic, retry - 1, topic_metadata.error_code)
      end
    end

    defp remove_stale_brokers(brokers, metadata_brokers) do
      {brokers_to_keep, brokers_to_remove} = Enum.partition(brokers, fn(broker) ->
        Enum.find_value(metadata_brokers, &(broker.host == &1.host && broker.port == &1.port && broker.socket && Port.info(broker.socket)))
      end)
      case length(brokers_to_keep) do
        0 -> brokers_to_remove
        _ -> Enum.each(brokers_to_remove, fn(broker) ->
          Logger.log(:info, "Closing connection to broker #{inspect broker.host} on port #{inspect broker.port}")
          KafkaEx.NetworkClient.close_socket(broker.socket)
        end)
          brokers_to_keep
      end
    end

    defp add_new_brokers(brokers, []), do: brokers
    defp add_new_brokers(brokers, [metadata_broker|metadata_brokers]) do
      case Enum.find(brokers, &(metadata_broker.host == &1.host && metadata_broker.port == &1.port)) do
        nil -> Logger.log(:info, "Establishing connection to broker #{inspect metadata_broker.host} on port #{inspect metadata_broker.port}")
          add_new_brokers([%{metadata_broker | socket: KafkaEx.NetworkClient.create_socket(metadata_broker.host, metadata_broker.port)} | brokers], metadata_brokers)
        _ -> add_new_brokers(brokers, metadata_brokers)
      end
    end

    defp first_broker_response(request, brokers, sync_timeout) do
      Enum.find_value(brokers, fn(broker) ->
        if Proto.Metadata.Broker.connected?(broker) do
          KafkaEx.NetworkClient.send_sync_request(broker, request, sync_timeout)
        end
      end)
    end
  end
end
