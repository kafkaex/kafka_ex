defmodule Kafka.Consumer.Worker do
  require Logger

  def start(broker_list, client_id, handler, topic, partition, offset) do
    Kafka.SimpleConsumer.new(broker_list, client_id, topic, partition)
    |> create_worker(handler, topic, partition, offset)
    |> consume(offset)
  end

  defp create_worker({:ok, consumer}, handler, topic, partition, offset) do
    case GenEvent.start_link do
      {:ok, manager} ->
        GenEvent.add_handler(manager, handler, [])
        %{
          :consumer => consumer,
          :event_mgr => manager,
          :topic     => topic,
          :partition => partition,
          :offset    => offset
        }
      {:error, reason} -> {:error, reason}
    end
  end

  defp create_worker({:error, reason, _connection}, _handler, topic, partition, _offset) do
    {:error, reason, topic, partition}
  end

  defp consume({:error, reason, topic, partition}, _offset) do
    Logger.error("Error consuming topic #{topic}, partition #{partition}: #{reason}")
    {:error, reason}
  end

  defp consume(worker, offset) do
    fetch_data(worker, offset)
    |> parse_result(worker)
    |> send_data_and_get_next
  end

  defp fetch_data(worker, offset) do
    Kafka.SimpleConsumer.fetch(worker.consumer, offset)
  end

  defp parse_result({:ok, result, consumer}, worker) do
    result[worker.topic][worker.partition]
    |> transform_error_code
    |> update_consumer_in_worker(consumer, worker)
  end

  defp parse_result({:error, reason, consumer}, worker) do
    {:error, reason, %{worker | :consumer => consumer}}
  end

  defp transform_error_code(parsed) do
    %{parsed | :error_code => Kafka.Protocol.error(parsed.error_code)}
  end

  defp update_consumer_in_worker(parsed, consumer, worker) do
    {parsed, %{worker | :consumer => consumer}}
  end

  defp send_data_and_get_next({:error, reason, worker}) do
    {:error, reason, worker}
  end

  defp send_data_and_get_next({%{:error_code => :no_error,
                                 :hw_mark_offset => hw_mark_offset,
                                 :message_set => message_set}, worker}) do
    offset = send_data(worker, message_set)
    if offset == hw_mark_offset-1, do: :timer.sleep(1000)
    consume(%{worker | :offset => offset}, offset + 1)
  end

  defp send_data_and_get_next({%{:error_code => :offset_out_of_range}, _worker}) do
    Logger.error("Offset out of range, exiting")
    {:error, :offset_out_of_range}
  end

  defp send_data(worker, []) do
    worker.offset
  end

  defp send_data(worker, messages) do
    for msg <- messages, do: GenEvent.notify(worker.event_mgr, msg)
    List.last(messages).offset
  end
end
