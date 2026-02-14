defmodule KafkaEx.Messages.Fetch do
  @moduledoc """
  Represents the result of a Fetch operation from Kafka.

  This struct contains the fetched records along with metadata about
  the fetch response such as high watermark, last stable offset, and
  throttling information.

  Field names align with Java Kafka client's `FetchResponseData.PartitionData`:
  - `high_watermark` - maximum offset available in the partition
  - `last_stable_offset` - last stable offset for transactions (V4+)
  - `log_start_offset` - earliest offset in the log (V5+)
  - `preferred_read_replica` - suggested replica for reading (V11+, KIP-392)
  - `aborted_transactions` - list of aborted transaction details (V4+)
  """

  alias KafkaEx.Messages.Fetch.Record

  defstruct [
    :topic,
    :partition,
    :records,
    :high_watermark,
    :last_offset,
    :last_stable_offset,
    :log_start_offset,
    :preferred_read_replica,
    :throttle_time_ms,
    :aborted_transactions
  ]

  @type aborted_transaction :: %{
          producer_id: integer(),
          first_offset: integer()
        }

  @type t :: %__MODULE__{
          topic: String.t(),
          partition: non_neg_integer(),
          records: [Record.t()],
          high_watermark: non_neg_integer(),
          last_offset: non_neg_integer() | nil,
          last_stable_offset: non_neg_integer() | nil,
          log_start_offset: non_neg_integer() | nil,
          preferred_read_replica: integer() | nil,
          throttle_time_ms: non_neg_integer() | nil,
          aborted_transactions: [aborted_transaction()] | nil
        }

  @doc """
  Builds a Fetch result struct from response data.
  """
  @spec build(Keyword.t()) :: t()
  def build(opts) do
    records = Keyword.fetch!(opts, :records)
    last_offset = compute_last_offset(records, Keyword.get(opts, :last_offset))

    %__MODULE__{
      topic: Keyword.fetch!(opts, :topic),
      partition: Keyword.fetch!(opts, :partition),
      records: records,
      high_watermark: Keyword.fetch!(opts, :high_watermark),
      last_offset: last_offset,
      last_stable_offset: Keyword.get(opts, :last_stable_offset),
      log_start_offset: Keyword.get(opts, :log_start_offset),
      preferred_read_replica: Keyword.get(opts, :preferred_read_replica),
      throttle_time_ms: Keyword.get(opts, :throttle_time_ms),
      aborted_transactions: Keyword.get(opts, :aborted_transactions)
    }
  end

  @doc """
  Returns true if there are no records in the fetch response.
  """
  @spec empty?(t()) :: boolean()
  def empty?(%__MODULE__{records: []}), do: true
  def empty?(%__MODULE__{}), do: false

  @doc """
  Returns the number of records in the fetch response.
  """
  @spec record_count(t()) :: non_neg_integer()
  def record_count(%__MODULE__{records: records}), do: length(records)

  @doc """
  Returns the next offset to fetch from (last_offset + 1 or high_watermark if empty).
  """
  @spec next_offset(t()) :: non_neg_integer()
  def next_offset(%__MODULE__{last_offset: nil, high_watermark: hw}), do: hw
  def next_offset(%__MODULE__{last_offset: last}), do: last + 1

  @doc """
  Filters records to only include those with offset >= the requested fetch offset.

  Kafka may return full RecordBatches that include records before the requested
  offset. This function trims those pre-offset records, matching the behavior
  of the Java Kafka client.
  """
  @spec filter_from_offset(t(), non_neg_integer()) :: t()
  def filter_from_offset(%__MODULE__{} = fetch, offset) do
    filtered = Enum.filter(fetch.records, &(&1.offset >= offset))
    last = compute_last_offset(filtered, nil)
    %{fetch | records: filtered, last_offset: last}
  end

  # Private helpers

  defp compute_last_offset([], _explicit_last), do: nil
  defp compute_last_offset(_records, explicit_last) when not is_nil(explicit_last), do: explicit_last

  defp compute_last_offset(records, _explicit_last) do
    records
    |> Enum.max_by(fn record -> record.offset end)
    |> Map.get(:offset)
  end
end
