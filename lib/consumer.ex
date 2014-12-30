defmodule Kafka.Consumer do
  use GenServer

  def init({broker_list, client_id}) do
    IO.inspect(Kafka.Connection.connect(broker_list, client_id))
  end
end
