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
  defp server("0.8.2"), do: KafkaEx.Server0P8P2
  defp server(_), do: KafkaEx.Server0P9P0

end
