defmodule Kafka do
  use Application

  def start(_type, _args) do
    Kafka.Supervisor.start_link
  end
end
