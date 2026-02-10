defmodule KafkaEx.Protocol.Kayrock.LeaveGroup.RequestHelpers do
  @moduledoc """
  Shared helper functions for building LeaveGroup requests across all versions.
  """

  @doc """
  Extracts common fields from request options.
  """
  @spec extract_common_fields(Keyword.t()) :: %{group_id: String.t(), member_id: String.t()}
  def extract_common_fields(opts) do
    %{
      group_id: Keyword.fetch!(opts, :group_id),
      member_id: Keyword.fetch!(opts, :member_id)
    }
  end
end
