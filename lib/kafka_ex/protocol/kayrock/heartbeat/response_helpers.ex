defmodule KafkaEx.Protocol.Kayrock.Heartbeat.ResponseHelpers do
  @moduledoc """
  Shared helper functions for parsing Heartbeat responses across all versions (V0-V4).

  Version differences:
  - V0: No throttle_time_ms field. Returns `{:ok, :no_error}` on success.
  - V1-V3: Adds throttle_time_ms to the response. Returns `{:ok, %Heartbeat{}}` on success.
  - V4: Flexible version (KIP-482) -- Kayrock handles compact encoding/decoding
        transparently. Domain-relevant fields are identical to V1-V3.
  """

  alias KafkaEx.Client.Error
  alias KafkaEx.Messages.Heartbeat
  alias Kayrock.ErrorCode

  @doc """
  Parses a V0 response (no throttle_time_ms).

  Returns `{:ok, :no_error}` on success to preserve the existing V0 contract.
  """
  @spec parse_v0_response(map()) :: {:ok, :no_error} | {:error, Error.t()}
  def parse_v0_response(response) do
    case ErrorCode.code_to_atom(response.error_code) do
      :no_error -> {:ok, :no_error}
      error_atom -> {:error, Error.build(error_atom, %{})}
    end
  end

  @doc """
  Parses a V1+ response (includes throttle_time_ms).

  Returns `{:ok, %Heartbeat{}}` on success with throttle_time_ms populated.
  """
  @spec parse_v1_plus_response(map()) :: {:ok, Heartbeat.t()} | {:error, Error.t()}
  def parse_v1_plus_response(response) do
    case ErrorCode.code_to_atom(response.error_code) do
      :no_error ->
        {:ok, Heartbeat.build(throttle_time_ms: response.throttle_time_ms)}

      error_atom ->
        {:error, Error.build(error_atom, %{})}
    end
  end
end
