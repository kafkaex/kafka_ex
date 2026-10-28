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

  @doc """
  Builds a V10 Metadata request.

  V10 adds a `topic_id` to every request topic. Per the Kafka spec, V10 and
  V11 must not look topics up by id (brokers never implemented it), so every
  topic is sent by name with the null (all-zero) topic id.
  """
  @spec build_v10_request(map(), Keyword.t()) :: map()
  def build_v10_request(request_template, opts) do
    request_template
    |> build_v8_plus_request(opts)
    |> Map.put(:topics, build_named_topics_with_null_id(opts))
  end

  @doc """
  Builds a V11 Metadata request.

  V11 drops `include_cluster_authorized_operations` (moved to DescribeCluster,
  KIP-700). Topics are still sent by name only, as in V10.
  """
  @spec build_v11_request(map(), Keyword.t()) :: map()
  def build_v11_request(request_template, opts) do
    request_template
    |> Map.put(:topics, build_named_topics_with_null_id(opts))
    |> Map.put(:allow_auto_topic_creation, allow_auto_topic_creation?(opts))
    |> Map.put(:include_topic_authorized_operations, Keyword.get(opts, :include_topic_authorized_operations, false))
  end

  @doc """
  Builds a V12+ Metadata request (V12, V13).

  Same fields as V11, but topics may also be looked up by id: every entry of
  the `:topic_ids` option (16-byte binaries) is sent as `%{topic_id: id, name: nil}`,
  alongside the named `:topics`. With neither option set, the request asks
  for all topics (`topics: nil`).
  """
  @spec build_v12_plus_request(map(), Keyword.t()) :: map()
  def build_v12_plus_request(request_template, opts) do
    topics =
      case {build_named_topics_with_null_id(opts), build_topic_id_list(opts)} do
        {nil, []} -> nil
        {nil, by_id} -> by_id
        {by_name, by_id} -> by_name ++ by_id
      end

    request_template
    |> build_v11_request(opts)
    |> Map.put(:topics, topics)
  end

  @doc """
  Returns the `:topic_ids` option as V12+ request topic entries.
  """
  @spec build_topic_id_list(Keyword.t()) :: [%{topic_id: <<_::128>>, name: nil}]
  def build_topic_id_list(opts) do
    opts
    |> Keyword.get(:topic_ids)
    |> List.wrap()
    |> Enum.map(fn <<_::128>> = topic_id -> %{topic_id: topic_id, name: nil} end)
  end

  defp build_named_topics_with_null_id(opts) do
    case build_topics_list(opts) do
      nil -> nil
      topics -> Enum.map(topics, &Map.put_new(&1, :topic_id, nil))
    end
  end
end
