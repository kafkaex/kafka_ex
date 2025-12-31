defmodule KafkaEx.Protocol.Kayrock.DeleteTopics.RequestHelpers do
  @moduledoc """
  Shared helper functions for building DeleteTopics requests across all versions.
  """

  @doc """
  Extracts common fields from request options.

  ## Required Options

  - `:topics` - List of topic names to delete
  - `:timeout` - Request timeout in milliseconds

  ## Returns

  A map with extracted fields.
  """
  @spec extract_common_fields(Keyword.t()) :: map()
  def extract_common_fields(opts) do
    %{
      topics: Keyword.fetch!(opts, :topics),
      timeout: Keyword.fetch!(opts, :timeout)
    }
  end
end
