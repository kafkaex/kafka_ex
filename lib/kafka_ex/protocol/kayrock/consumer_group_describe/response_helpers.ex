defmodule KafkaEx.Protocol.Kayrock.ConsumerGroupDescribe.ResponseHelpers do
  @moduledoc """
  Shared helper functions for parsing ConsumerGroupDescribe responses (V0-V1).

  V1 adds `member_type` per member; `ConsumerGroupDescription.Member` reads it
  with `Map.get/2`, so it is `nil` for V0 responses.
  """

  alias KafkaEx.Messages.ConsumerGroupDescription
  alias Kayrock.ErrorCode

  @doc """
  Returns `{:ok, [ConsumerGroupDescription.t()]}` when every group succeeded,
  or `{:error, [{group_id, error_atom}]}` for the failed groups.
  """
  @spec parse_response(map()) :: {:ok, [ConsumerGroupDescription.t()]} | {:error, [{String.t(), atom()}]}
  def parse_response(%{groups: groups}) do
    case Enum.filter(groups, &(&1.error_code != 0)) do
      [] ->
        {:ok, Enum.map(groups, &ConsumerGroupDescription.from_consumer_group_describe_response/1)}

      errors ->
        {:error,
         Enum.map(errors, fn %{group_id: group_id, error_code: code} -> {group_id, ErrorCode.code_to_atom(code)} end)}
    end
  end
end
