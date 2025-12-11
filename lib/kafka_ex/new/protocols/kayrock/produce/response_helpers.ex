defmodule KafkaEx.New.Protocols.Kayrock.Produce.ResponseHelpers do
  @moduledoc """
  Shared utility functions for parsing Produce responses.

  These are low-level utilities used by the version-specific response
  implementations. Each version implementation handles its own field
  extraction logic directly.
  """

  alias KafkaEx.New.Client.Error
  alias KafkaEx.New.Kafka.RecordMetadata

  alias Kayrock.ErrorCode

  @doc """
  Extracts the first topic and partition response from a Produce response.

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
    error_code = Map.get(partition_response, :error_code, 0)

    case ErrorCode.code_to_atom(error_code) do
      :no_error ->
        {:ok, partition_response}

      error_atom ->
        {:error, Error.build(error_atom, %{topic: topic, partition: partition_response.partition})}
    end
  end

  @doc """
  Builds a RecordMetadata struct from parsed response data.
  """
  @spec build_record_metadata(Keyword.t()) :: RecordMetadata.t()
  def build_record_metadata(opts) do
    RecordMetadata.build(opts)
  end

  @doc """
  Builds an error response for empty responses.
  """
  @spec empty_response_error() :: {:error, Error.t()}
  def empty_response_error do
    {:error, Error.build(:empty_response, %{})}
  end

  @doc """
  Common parsing logic for produce responses.

  Extracts the first partition response, checks for errors, and builds
  the RecordMetadata using the provided field extractor function.

  The `field_extractor` function receives `{response, partition_resp}` and
  should return keyword options for `build_record_metadata/1`.
  """
  @spec parse_response(map(), (map(), map() -> Keyword.t())) ::
          {:ok, RecordMetadata.t()} | {:error, Error.t()}
  def parse_response(response, field_extractor) when is_function(field_extractor, 2) do
    with {:ok, topic, partition_resp} <- extract_first_partition_response(response),
         {:ok, partition_resp} <- check_error(topic, partition_resp) do
      opts =
        [topic: topic, partition: partition_resp.partition, base_offset: partition_resp.base_offset]
        |> Keyword.merge(field_extractor.(response, partition_resp))

      {:ok, build_record_metadata(opts)}
    else
      {:error, :empty_response} -> empty_response_error()
      {:error, _} = error -> error
    end
  end
end
