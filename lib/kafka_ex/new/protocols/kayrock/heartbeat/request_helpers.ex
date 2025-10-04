defmodule KafkaEx.New.Protocols.Kayrock.Heartbeat.RequestHelpers do
  @moduledoc """
  Shared helper functions for building Heartbeat requests across all versions.
  """

  @doc """
  Extracts common fields from request options.
  """
  @spec extract_common_fields(Keyword.t()) :: %{
          group_id: String.t(),
          member_id: String.t(),
          generation_id: non_neg_integer()
        }
  def extract_common_fields(opts) do
    %{
      group_id: Keyword.fetch!(opts, :group_id),
      member_id: Keyword.fetch!(opts, :member_id),
      generation_id: Keyword.fetch!(opts, :generation_id)
    }
  end
end
