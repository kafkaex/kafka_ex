defmodule KafkaEx.Protocol.Kayrock.OffsetFetch.RequestHelpers do
  @moduledoc """
  Shared helper functions for building OffsetFetch requests across all versions.

  This module reduces code duplication while maintaining type safety through
  protocol dispatch for each version-specific struct.
  """

  @type partition_data :: %{partition_num: non_neg_integer()}
  @type topic_data :: {String.t(), [partition_data()]}
  @type partition_output :: %{partition: non_neg_integer()}
  @type topic_output :: %{topic: String.t(), partitions: [partition_output()]}

  @doc """
  Builds the topics structure for OffsetFetch requests.
  """
  @spec build_topics(Keyword.t()) :: [topic_output()]
  def build_topics(opts) do
    opts
    |> Keyword.fetch!(:topics)
    |> Enum.map(fn {topic, partitions} ->
      %{
        name: topic,
        partition_indexes: build_partition_indexes(partitions)
      }
    end)
  end

  @doc """
  Builds partition index list for a topic.
  """
  @spec build_partition_indexes([partition_data()]) :: [non_neg_integer()]
  def build_partition_indexes(partitions) do
    Enum.map(partitions, fn partition_data ->
      partition_data.partition_num
    end)
  end

  @doc """
  Extracts common fields from request options.
  """
  @spec extract_common_fields(Keyword.t()) :: %{group_id: String.t()}
  def extract_common_fields(opts) do
    %{
      group_id: Keyword.fetch!(opts, :group_id)
    }
  end

  @doc """
  Builds an OffsetFetch V0-V6 request (`group_id` + `topics`).
  """
  @spec build_request(struct(), Keyword.t()) :: struct()
  def build_request(request_template, opts) do
    %{group_id: group_id} = extract_common_fields(opts)

    request_template
    |> Map.put(:group_id, group_id)
    |> Map.put(:topics, build_topics(opts))
  end

  @doc """
  Builds an OffsetFetch V7 request: V6 plus `require_stable` (KIP-447).

  `:require_stable` defaults to `false` (the broker returns the latest
  committed offset even while a transactional commit is pending), matching
  what earlier versions implicitly did.
  """
  @spec build_v7_request(struct(), Keyword.t()) :: struct()
  def build_v7_request(request_template, opts) do
    request_template
    |> build_request(opts)
    |> Map.put(:require_stable, require_stable?(opts))
  end

  @doc """
  Builds an OffsetFetch V8 request: the single group goes into the batched
  `groups` array (KIP-709).
  """
  @spec build_v8_request(struct(), Keyword.t()) :: struct()
  def build_v8_request(request_template, opts) do
    %{group_id: group_id} = extract_common_fields(opts)
    group = %{group_id: group_id, topics: build_topics(opts)}

    request_template
    |> Map.put(:groups, [group])
    |> Map.put(:require_stable, require_stable?(opts))
  end

  @doc """
  Builds an OffsetFetch V9 request: V8 plus per-group `member_id` /
  `member_epoch` (KIP-848).

  These are only validated by the broker for consumer-protocol groups. They
  are taken from `:member_id` / `:member_epoch`; without a `:member_epoch` the
  request is sent as a classic/standalone fetch (`member_id: nil`,
  `member_epoch: -1`).
  """
  @spec build_v9_request(struct(), Keyword.t()) :: struct()
  def build_v9_request(request_template, opts) do
    request_template
    |> build_v8_request(opts)
    |> Map.update!(:groups, fn groups -> Enum.map(groups, &Map.merge(&1, member_fields(opts))) end)
  end

  @doc """
  Builds an OffsetFetch V10 request: V9 with topics identified by `topic_id`
  instead of name (KIP-848).

  The `:topic_ids` option (`%{topic_name => <<_::128>>}`) must cover every
  requested topic; `KafkaEx.Client.RequestBuilder` fills it from cluster
  metadata. A topic with no id raises `ArgumentError`.
  """
  @spec build_v10_request(struct(), Keyword.t()) :: struct()
  def build_v10_request(request_template, opts) do
    topic_ids = Keyword.get(opts, :topic_ids, %{})

    topics =
      opts
      |> Keyword.fetch!(:topics)
      |> Enum.map(fn {topic, partitions} ->
        %{
          topic_id: fetch_topic_id!(topic_ids, topic),
          partition_indexes: build_partition_indexes(partitions)
        }
      end)

    request_template
    |> build_v9_request(opts)
    |> Map.update!(:groups, fn groups -> Enum.map(groups, &Map.put(&1, :topics, topics)) end)
  end

  defp fetch_topic_id!(topic_ids, topic) do
    case Map.get(topic_ids, topic) do
      <<_::128>> = topic_id ->
        topic_id

      _ ->
        raise ArgumentError, "no topic id known for topic #{inspect(topic)}; OffsetFetch v10+ identifies topics by id"
    end
  end

  defp require_stable?(opts), do: Keyword.get(opts, :require_stable, false)

  defp member_fields(opts) do
    case Keyword.get(opts, :member_epoch) do
      nil -> %{member_id: nil, member_epoch: -1}
      member_epoch -> %{member_id: Keyword.get(opts, :member_id), member_epoch: member_epoch}
    end
  end
end
