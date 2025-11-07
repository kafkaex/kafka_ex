defmodule KafkaEx.New.Protocols.Kayrock.Metadata.RequestHelpers do
  @moduledoc """
  Helper functions for building Metadata requests across different protocol versions.
  """

  @doc """
  Extracts and normalizes topics list from options.
  """
  @spec build_topics_list(Keyword.t()) :: [String.t()] | nil
  def build_topics_list(opts) when is_list(opts) do
    case Keyword.get(opts, :topics) do
      nil -> nil
      [] -> nil
      topics when is_list(topics) -> topics
    end
  end

  @doc """
  Returns the default API version for Metadata requests.
  """
  @spec default_api_version() :: 0 | 1 | 2
  def default_api_version, do: 1

  @doc """
  Checks if auto topic creation should be enabled.
  """
  @spec allow_auto_topic_creation?(Keyword.t()) :: boolean()
  def allow_auto_topic_creation?(opts) do
    Keyword.get(opts, :allow_auto_topic_creation, false)
  end
end
