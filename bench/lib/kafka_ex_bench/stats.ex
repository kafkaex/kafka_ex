defmodule KafkaExBench.Stats do
  def percentile([], _p), do: nil

  def percentile(samples, p) when is_integer(p) and p >= 0 and p <= 100 do
    sorted = Enum.sort(samples)
    rank = max(div(p * length(sorted) + 99, 100), 1)
    Enum.at(sorted, rank - 1)
  end

  def median([]), do: nil

  def median(values) do
    sorted = Enum.sort(values)
    n = length(sorted)
    mid = div(n, 2)
    if rem(n, 2) == 1, do: Enum.at(sorted, mid), else: (Enum.at(sorted, mid - 1) + Enum.at(sorted, mid)) / 2
  end

  def cv(values) when length(values) < 2, do: nil

  def cv(values) do
    n = length(values)
    mean = Enum.sum(values) / n

    if mean == 0 do
      nil
    else
      variance = Enum.reduce(values, 0, fn v, acc -> acc + (v - mean) ** 2 end) / (n - 1)
      :math.sqrt(variance) / mean
    end
  end
end
