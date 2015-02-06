defmodule Kafka.Consumer do
  def new(broker_list, client_id) do
    {:ok, %{:broker_list => broker_list, :client_id => client_id}}
  end

  def subscribe(consumer, handler, topic, partition \\ :all, offset \\ :latest) do
    Task.start_link(Kafka.Consumer.Worker, :start, [consumer.broker_list, consumer.client_id, handler, topic, partition, offset])
  end
end
