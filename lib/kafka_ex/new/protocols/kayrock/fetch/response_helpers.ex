defmodule KafkaEx.New.Protocols.Kayrock.Fetch.ResponseHelpers do
  @moduledoc """
  Shared utility functions for parsing Fetch responses.

  These are low-level utilities used by the version-specific response
  implementations. Handles both MessageSet (V0-V3) and RecordBatch (V4+)
  message formats.
  """

  alias KafkaEx.New.Client.Error
  alias KafkaEx.New.Kafka.Fetch
  alias KafkaEx.New.Kafka.Fetch.Record

  alias Kayrock.ErrorCode

  @doc """
  Extracts the first topic and partition response from a Fetch response.

  Returns `{:ok, topic, partition_response}` or `{:error, :empty_response}`.
  """
  @spec extract_first_partition_response(map()) ::
          {:ok, String.t(), map()} | {:error, :empty_response}
  def extract_first_partition_response(%{responses: [topic_response | _]}) do
    %{topic: topic, partition_responses: [partition_response | _]} = topic_response
    {:ok, topic, partition_response}
  end

  def extract_first_partition_response(%{responses: []}) do
    {:error, :empty_response}
  end

  def extract_first_partition_response(_) do
    {:error, :empty_response}
  end

  @doc """
  Checks if the partition response has an error and returns the appropriate result.

  Returns `{:ok, partition_response}` if no error, `{:error, Error.t()}` otherwise.
  """
  @spec check_error(String.t(), map()) :: {:ok, map()} | {:error, Error.t()}
  def check_error(topic, partition_response) do
    partition_header = Map.get(partition_response, :partition_header, %{})
    error_code = Map.get(partition_header, :error_code, 0)

    case ErrorCode.code_to_atom(error_code) do
      :no_error ->
        {:ok, partition_response}

      error_atom ->
        partition = Map.get(partition_header, :partition, 0)
        {:error, Error.build(error_atom, %{topic: topic, partition: partition})}
    end
  end

  @doc """
  Converts record_set (MessageSet or RecordBatch) to list of Record structs.
  """
  @spec convert_records(term(), String.t(), non_neg_integer()) :: [Record.t()]
  def convert_records(nil, _topic, _partition), do: []

  def convert_records(%Kayrock.MessageSet{messages: messages}, topic, partition) do
    Enum.map(messages, fn msg ->
      Record.build(
        offset: msg.offset,
        key: msg.key,
        value: msg.value,
        timestamp: msg.timestamp,
        timestamp_type: extract_timestamp_type(msg.attributes),
        attributes: msg.attributes,
        crc: msg.crc,
        topic: topic,
        partition: partition
      )
    end)
  end

  def convert_records(record_batches, topic, partition) when is_list(record_batches) do
    Enum.flat_map(record_batches, fn record_batch ->
      timestamp_type = extract_timestamp_type_from_batch(record_batch)

      Enum.map(record_batch.records, fn record ->
        Record.build(
          offset: record.offset,
          key: record.key,
          value: record.value,
          timestamp: record.timestamp,
          timestamp_type: timestamp_type,
          headers: convert_headers(record.headers),
          attributes: record.attributes,
          topic: topic,
          partition: partition
        )
      end)
    end)
  end

  @doc """
  Converts Kayrock RecordHeader structs to {key, value} tuples.
  """
  @spec convert_headers(list() | nil) :: [{binary(), binary()}] | nil
  def convert_headers(nil), do: nil
  def convert_headers([]), do: []

  def convert_headers(headers) when is_list(headers) do
    Enum.map(headers, fn header ->
      {header.key, header.value}
    end)
  end

  @doc """
  Computes the last offset from a list of records.
  """
  @spec compute_last_offset([Record.t()]) :: non_neg_integer() | nil
  def compute_last_offset([]), do: nil

  def compute_last_offset(records) do
    records
    |> Enum.max_by(fn record -> record.offset end)
    |> Map.get(:offset)
  end

  @doc """
  Builds a Fetch struct from parsed response data.
  """
  @spec build_fetch(Keyword.t()) :: Fetch.t()
  def build_fetch(opts) do
    Fetch.build(opts)
  end

  @doc """
  Builds an error response for empty responses.
  """
  @spec empty_response_error() :: {:error, Error.t()}
  def empty_response_error do
    {:error, Error.build(:empty_response, %{})}
  end

  @doc """
  Field extractor for V5+ responses (V5, V6, V7).

  These versions share the same response fields:
  - throttle_time_ms
  - last_stable_offset
  - log_start_offset
  - aborted_transactions
  """
  @spec extract_v5_plus_fields(map(), map()) :: Keyword.t()
  def extract_v5_plus_fields(response, partition_resp) do
    partition_header = Map.get(partition_resp, :partition_header, %{})

    [
      throttle_time_ms: Map.get(response, :throttle_time_ms, 0),
      last_stable_offset: Map.get(partition_header, :last_stable_offset),
      log_start_offset: Map.get(partition_header, :log_start_offset),
      aborted_transactions: Map.get(partition_header, :aborted_transactions)
    ]
  end

  @doc """
  Common parsing logic for fetch responses.

  Extracts the first partition response, checks for errors, converts records,
  and builds the Fetch struct using the provided field extractor function.

  The `field_extractor` function receives `{response, partition_resp}` and
  should return keyword options for additional fields (throttle_time_ms, etc).
  """
  @spec parse_response(map(), (map(), map() -> Keyword.t())) ::
          {:ok, Fetch.t()} | {:error, Error.t()}
  def parse_response(response, field_extractor) when is_function(field_extractor, 2) do
    with {:ok, topic, partition_resp} <- extract_first_partition_response(response),
         {:ok, partition_resp} <- check_error(topic, partition_resp) do
      partition_header = Map.get(partition_resp, :partition_header, %{})
      partition = Map.get(partition_header, :partition, 0)
      high_watermark = Map.get(partition_header, :high_watermark, 0)
      record_set = Map.get(partition_resp, :record_set)

      records = convert_records(record_set, topic, partition)
      last_offset = compute_last_offset(records)

      opts =
        [
          topic: topic,
          partition: partition,
          records: records,
          high_watermark: high_watermark,
          last_offset: last_offset
        ]
        |> Keyword.merge(field_extractor.(response, partition_resp))

      {:ok, build_fetch(opts)}
    else
      {:error, :empty_response} -> empty_response_error()
      {:error, _} = error -> error
    end
  end

  # Private helpers for extracting timestamp type from attributes

  # Bit 3 of attributes indicates timestamp type:
  # 0 = CreateTime, 1 = LogAppendTime
  defp extract_timestamp_type(nil), do: nil

  defp extract_timestamp_type(attributes) when is_integer(attributes) do
    case Bitwise.band(Bitwise.bsr(attributes, 3), 1) do
      0 -> :create_time
      1 -> :log_append_time
    end
  end

  defp extract_timestamp_type(_), do: nil

  # For RecordBatch, timestamp type is in the batch attributes
  defp extract_timestamp_type_from_batch(%{attributes: attributes}) do
    extract_timestamp_type(attributes)
  end

  defp extract_timestamp_type_from_batch(_), do: nil
end
