defmodule KafkaEx.Partitioner do
  @moduledoc """
  Behaviour definition for partitioners, that assigns partitions for requests.
  """

  alias KafkaEx.Protocol.Produce.Request, as: ProduceRequest
  alias KafkaEx.Protocol.Metadata.Response, as: MetadataResponse

  @callback assign_partition(request :: ProduceRequest.t(), metadata :: MetadataResponse.t()) ::
              ProduceRequest.t()
  defmacro __using__(_) do
    quote location: :keep do
      @behaviour KafkaEx.Partitioner
    end
  end

  @doc """
  Returns key for given messages

  Function looks for message key in messages list of {ProduceRequest}. It may return
  either `{:ok, nil}` if no key was found, `{:ok, key}` when key was found,
  or `{:error, atom}` when error happens while looking for the key.
  """
  @spec get_key(request :: ProduceRequest.t()) ::
          {:ok, nil | binary} | {:error, atom}
  def get_key(%ProduceRequest{messages: messages}) when length(messages) > 0 do
    case unique_keys(messages) do
      [key] -> {:ok, key}
      _ -> {:error, :inconsistent_keys}
    end
  end

  def get_key(_) do
    {:error, :no_messages}
  end

  defp unique_keys(messages) do
    messages
    |> Enum.map(&Map.get(&1, :key))
    |> Enum.uniq()
  end
end
