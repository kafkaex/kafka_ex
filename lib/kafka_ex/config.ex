defmodule KafkaEx.Config do
  @moduledoc false

  def default_worker do
    :kafka_ex
  end

  def server_impl do
    :kafka_ex
      |> Application.get_env(:kafka_version, :default)
      |> server
  end

  defp server("0.8.0"), do: KafkaEx.Server0P8P0
  defp server(_), do: KafkaEx.DefaultServer

end
