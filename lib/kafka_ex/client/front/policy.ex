defmodule KafkaEx.Client.Front.Policy do
  @moduledoc false

  alias KafkaEx.Client.NodeSelector
  alias KafkaEx.Client.RequestContext
  alias KafkaEx.Support.Retry

  @type mode :: :retrying | :send_request | :acks0
  @type gate :: {binary(), non_neg_integer()} | nil
  @type key :: binary() | {:coordinator, binary()} | :brokers

  @enforce_keys [:selector, :mode, :max_attempts]
  defstruct [
    :selector,
    :mode,
    :max_attempts,
    gate: nil,
    retryable?: &Retry.data_plane_retryable?/1,
    send_once?: &Retry.transport_timeout?/1,
    rediscover_coordinator?: false,
    allow_auto_topic_creation: false
  ]

  @type t :: %__MODULE__{
          selector: NodeSelector.t(),
          mode: mode(),
          max_attempts: pos_integer(),
          gate: gate(),
          retryable?: (atom() -> boolean()),
          send_once?: (atom() -> boolean()),
          rediscover_coordinator?: boolean(),
          allow_auto_topic_creation: boolean()
        }

  @spec from_context(RequestContext.t(), keyword()) :: t()
  def from_context(%RequestContext{} = ctx, opts) do
    validate!(%__MODULE__{
      selector: ctx.node_selector,
      mode: :retrying,
      max_attempts: Keyword.fetch!(opts, :max_attempts),
      gate: Keyword.get(opts, :gate),
      retryable?: ctx.retryable?,
      send_once?: ctx.send_once?,
      rediscover_coordinator?: ctx.rediscover_coordinator?,
      allow_auto_topic_creation: Keyword.get(opts, :allow_auto_topic_creation, false)
    })
  end

  @spec send_request(NodeSelector.t()) :: t()
  def send_request(%NodeSelector{} = selector) do
    %__MODULE__{selector: selector, mode: :send_request, max_attempts: 1, retryable?: &never/1}
  end

  @spec acks0(NodeSelector.t()) :: t()
  def acks0(%NodeSelector{topic: topic, partition: partition} = selector) do
    %__MODULE__{selector: selector, mode: :acks0, max_attempts: 1, gate: {topic, partition}, retryable?: &never/1}
  end

  @spec coordinator?(t()) :: boolean()
  def coordinator?(%__MODULE__{selector: %NodeSelector{strategy: strategy}}), do: strategy == :consumer_group

  @spec partition?(t()) :: boolean()
  def partition?(%__MODULE__{selector: %NodeSelector{strategy: strategy}}) do
    strategy in [:topic_partition, :topic_partition_replica]
  end

  @spec first_available?(t()) :: boolean()
  def first_available?(%__MODULE__{selector: %NodeSelector{strategy: strategy}}), do: strategy == :first_available

  @spec key(t()) :: key()
  def key(%__MODULE__{selector: %NodeSelector{strategy: :consumer_group, consumer_group_name: group}}) do
    {:coordinator, group}
  end

  def key(%__MODULE__{selector: %NodeSelector{strategy: strategy, topic: topic}})
      when strategy in [:topic_partition, :topic_partition_replica] do
    topic
  end

  def key(%__MODULE__{}), do: :brokers

  defp never(_error), do: false

  # A gated dispatch may still be on the wire after :timeout, so a retry would put two on it.
  defp validate!(%__MODULE__{gate: {_, _}, retryable?: retryable?} = policy) do
    if retryable?.(:timeout), do: raise(ArgumentError, "a gated request must not retry :timeout"), else: policy
  end

  defp validate!(policy), do: policy
end
