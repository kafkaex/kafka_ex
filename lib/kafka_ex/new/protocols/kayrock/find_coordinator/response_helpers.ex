defmodule KafkaEx.New.Protocols.Kayrock.FindCoordinator.ResponseHelpers do
  @moduledoc """
  Shared utility functions for parsing FindCoordinator responses.
  """

  alias KafkaEx.New.Client.Error
  alias KafkaEx.New.Kafka.Broker
  alias KafkaEx.New.Kafka.FindCoordinator
  alias Kayrock.ErrorCode

  @doc """
  Parses coordinator data from response into a Broker struct.

  Returns nil if coordinator is nil.
  """
  @spec parse_coordinator(map() | nil) :: Broker.t() | nil
  def parse_coordinator(nil), do: nil

  def parse_coordinator(coordinator) do
    %Broker{
      node_id: coordinator.node_id,
      host: coordinator.host,
      port: coordinator.port
    }
  end

  @doc """
  Checks the error code and returns success or error result.

  Returns `{:ok, parsed_fields}` if no error, `{:error, Error.t()}` otherwise.
  """
  @spec check_error(integer(), Keyword.t()) :: {:ok, Keyword.t()} | {:error, Error.t()}
  def check_error(error_code, fields) do
    case ErrorCode.code_to_atom(error_code) do
      :no_error ->
        {:ok, Keyword.put(fields, :error_code, :no_error)}

      error_atom ->
        {:error, Error.build(error_atom, %{})}
    end
  end

  @doc """
  Parses a V0 response (no throttle_time_ms, no error_message).
  """
  @spec parse_v0_response(map()) :: {:ok, FindCoordinator.t()} | {:error, Error.t()}
  def parse_v0_response(response) do
    coordinator = parse_coordinator(response.coordinator)
    fields = [coordinator: coordinator]

    case check_error(response.error_code, fields) do
      {:ok, fields} ->
        {:ok, FindCoordinator.build(fields)}

      {:error, _} = error ->
        error
    end
  end

  @doc """
  Parses a V1 response (includes throttle_time_ms and error_message).
  """
  @spec parse_v1_response(map()) :: {:ok, FindCoordinator.t()} | {:error, Error.t()}
  def parse_v1_response(response) do
    coordinator = parse_coordinator(response.coordinator)

    fields = [
      coordinator: coordinator,
      throttle_time_ms: response.throttle_time_ms,
      error_message: response.error_message
    ]

    case check_error(response.error_code, fields) do
      {:ok, fields} ->
        {:ok, FindCoordinator.build(fields)}

      {:error, _} = error ->
        error
    end
  end
end
