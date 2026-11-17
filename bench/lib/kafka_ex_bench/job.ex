defmodule KafkaExBench.Job do
  @default_min_samples 500
  @default_metrics [
    {:throughput_ops, :higher_better},
    {:p50_us, :lower_better},
    {:client_reductions_per_op, :lower_better},
    {:vm_reductions_per_op, :lower_better}
  ]

  @type t :: %__MODULE__{}
  @type background :: %{
          start: (-> term()),
          stop: (term() -> {:ok, non_neg_integer()} | {:error, String.t(), non_neg_integer()})
        }
  @type metrics :: [{atom(), :higher_better | :lower_better}]

  @enforce_keys [:name, :parallel, :fun]
  defstruct [:name, :parallel, :fun, min_samples: @default_min_samples, background: nil, metrics: @default_metrics]

  def default_min_samples, do: @default_min_samples
  def default_metrics, do: @default_metrics
end
