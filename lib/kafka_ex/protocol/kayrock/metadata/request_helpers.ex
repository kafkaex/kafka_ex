defmodule KafkaEx.Protocol.Kayrock.Metadata.RequestHelpers do
  @moduledoc """
  Helper functions for building Metadata requests across different protocol versions.
  """

  @doc """
  Extracts and normalizes topics list from options.

  Wraps plain string topics as `%{name: topic}` maps, which is the format
  Kayrock's serializer expects for all Metadata API versions.
  """
  @spec build_topics_list(Keyword.t()) :: [%{name: String.t()}] | nil
  def build_topics_list(opts) when is_list(opts) do
    case Keyword.get(opts, :topics) do
      nil -> nil
      [] -> nil
      topics when is_list(topics) -> Enum.map(topics, &wrap_topic/1)
    end
  end

  defp wrap_topic(topic) when is_binary(topic), do: %{name: topic}
  defp wrap_topic(%{name: _} = topic), do: topic

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

  @doc """
  Builds a V8+ Metadata request with authorized operations fields.

  V8 adds `include_cluster_authorized_operations` and `include_topic_authorized_operations`.
  V9 uses the same fields with compact encoding (handled by Kayrock).
  """
  @spec build_v8_plus_request(map(), Keyword.t()) :: map()
  def build_v8_plus_request(request_template, opts) do
    topics = build_topics_list(opts)
    allow_auto = allow_auto_topic_creation?(opts)
    include_cluster_ops = Keyword.get(opts, :include_cluster_authorized_operations, false)
    include_topic_ops = Keyword.get(opts, :include_topic_authorized_operations, false)

    request_template
    |> Map.put(:topics, topics)
    |> Map.put(:allow_auto_topic_creation, allow_auto)
    |> Map.put(:include_cluster_authorized_operations, include_cluster_ops)
    |> Map.put(:include_topic_authorized_operations, include_topic_ops)
  end
end
