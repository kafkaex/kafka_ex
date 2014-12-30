defmodule Kafka do
  defp parse_broker_list(list) do
    Enum.map(String.split(list, ","), fn(x) -> String.split(x, ":") end)
  end
end
