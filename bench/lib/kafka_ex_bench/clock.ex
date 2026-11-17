defmodule KafkaExBench.Clock do
  def now_iso8601, do: DateTime.utc_now() |> DateTime.truncate(:second) |> DateTime.to_iso8601()
end
