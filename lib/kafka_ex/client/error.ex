defmodule KafkaEx.Client.Error do
  @moduledoc """
  This module represents kafka error as a struct with code and metadata
  """
  @protocol Application.compile_env(:kafka_ex, :protocol, KafkaEx.Protocol.KayrockProtocol)

  defstruct error: nil, metadata: %{}

  @type error_code :: KafkaEx.Support.Types.error_code() | atom

  @type t :: %__MODULE__{error: atom | nil, metadata: term}

  @spec build(error_code, term) :: __MODULE__.t()
  def build(error_code, metadata) when is_integer(error_code) do
    %__MODULE__{error: @protocol.error_code_to_atom(error_code), metadata: metadata}
  end

  def build(error, metadata) when is_atom(error) do
    %__MODULE__{error: error, metadata: metadata}
  end
end
