defmodule KafkaEx.Client.ResponseParser do
  @moduledoc """
  Reads a broker's response frame for `KafkaEx.Client`; parsing it into a result is
  `parse_response/2,3` of the protocol.
  """
  require Logger

  @protocol Application.compile_env(:kafka_ex, :protocol, KafkaEx.Protocol.KayrockProtocol)

  @doc false
  @spec deserialize(binary(), term()) :: {:ok, term()} | {:error, :parse_error}
  def deserialize(frame, request) do
    {response, _rest} = @protocol.response_deserializer(request).(frame)
    {:ok, response}
  rescue
    error ->
      Logger.error(
        "Failed to parse a response from the server: " <>
          inspect(frame, limit: :infinity) <>
          " for request #{inspect(request, limit: :infinity)} " <>
          "error: #{inspect(error)}"
      )

      {:error, :parse_error}
  end

  @doc """
  Per-topic errors of a Metadata response, by topic name
  """
  @spec metadata_topic_errors(term) :: %{String.t() => atom()}
  def metadata_topic_errors(response) do
    @protocol.metadata_topic_errors(response)
  end
end
