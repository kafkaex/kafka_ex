defmodule Kafka.Consumer do
  use GenServer

  def init({broker_list, client_id}) do
    {:ok, IO.inspect(Kafka.Connection.connect(broker_list, client_id))}
  end
end
