defmodule KafkaEx.New.Protocols.Kayrock.Produce.ResponseHelpers do
  @moduledoc """
  Shared utility functions for parsing Produce responses.

  These are low-level utilities used by the version-specific response
  implementations. Each version implementation handles its own field
  extraction logic directly.
  """

  alias KafkaEx.New.Structs.Error
  alias KafkaEx.New.Structs.Produce

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
  Builds a Produce struct from parsed response data.
  """
  @spec build_produce(Keyword.t()) :: Produce.t()
  def build_produce(opts) do
    Produce.build(opts)
  end

  @doc """
  Builds an error response for empty responses.
  """
  @spec empty_response_error() :: {:error, Error.t()}
  def empty_response_error do
    {:error, Error.build(:empty_response, %{})}
  end
end
