defmodule KafkaEx.Protocol.Kayrock.ConsumerGroupHeartbeat.RequestHelpers do
  @moduledoc """
  Shared helper functions for building ConsumerGroupHeartbeat requests (V0-V1).

  Version differences:
  - V0: `group_id`, `member_id`, `member_epoch`, `instance_id`, `rack_id`,
    `rebalance_timeout_ms`, `subscribed_topic_names`, `server_assignor`,
    `topic_partitions`
  - V1: +`subscribed_topic_regex`

  KIP-848 encodes "unchanged since the last heartbeat" as `null` (`-1` for
  `rebalance_timeout_ms`). Kayrock's request structs default the compact arrays
  to `[]`, which the broker reads as "subscribed to nothing" / "owns nothing",
  so every optional field is set explicitly here, defaulting to `nil`.

  ## Member id

  On V0 a joining member may send `""` and the coordinator assigns an id. From
  V1 (KIP-1082) the client generates the id and keeps it for its lifetime: when
  a V1+ join (`member_epoch: 0`) has no `member_id` (absent, `nil` or `""`),
  `generate_member_id/0` provides one. The response echoes it back as
  `KafkaEx.Messages.ConsumerGroupHeartbeat.member_id`, which the caller must
  send on every later heartbeat.
  """

  @unchanged_rebalance_timeout -1
  @join_epoch 0

  @doc """
  Builds a V0 request: every field except `subscribed_topic_regex`.

  `member_epoch` is required; `member_id` defaults to `""`.
  """
  @spec build_v0_request(map(), Keyword.t()) :: map()
  def build_v0_request(request_template, opts) do
    %{
      request_template
      | group_id: Keyword.fetch!(opts, :group_id),
        member_id: Keyword.get(opts, :member_id) || "",
        member_epoch: Keyword.fetch!(opts, :member_epoch),
        instance_id: Keyword.get(opts, :instance_id),
        rack_id: Keyword.get(opts, :rack_id),
        rebalance_timeout_ms: Keyword.get(opts, :rebalance_timeout_ms) || @unchanged_rebalance_timeout,
        subscribed_topic_names: Keyword.get(opts, :subscribed_topic_names),
        server_assignor: Keyword.get(opts, :server_assignor),
        topic_partitions: build_topic_partitions(Keyword.get(opts, :topic_partitions))
    }
  end

  @doc """
  Builds a V1 request: V0 fields plus `subscribed_topic_regex`.
  """
  @spec build_v1_plus_request(map(), Keyword.t()) :: map()
  def build_v1_plus_request(request_template, opts) do
    request_template
    |> build_v0_request(opts)
    |> Map.put(:subscribed_topic_regex, Keyword.get(opts, :subscribed_topic_regex))
    |> put_client_member_id()
  end

  @doc """
  Generates a member id the way the Java client does (KIP-1082): a random
  128-bit UUID, URL-safe base64 without padding (22 characters).
  """
  @spec generate_member_id() :: String.t()
  def generate_member_id do
    <<a::48, _::4, b::12, _::2, c::62>> = :crypto.strong_rand_bytes(16)
    Base.url_encode64(<<a::48, 4::4, b::12, 2::2, c::62>>, padding: false)
  end

  defp put_client_member_id(%{member_id: "", member_epoch: @join_epoch} = request) do
    %{request | member_id: generate_member_id()}
  end

  defp put_client_member_id(request), do: request

  @doc """
  Normalises owned partitions to the wire shape, `[%{topic_id, partitions}]`.

  Accepts maps or `KafkaEx.Messages.TopicPartitions` structs with `:topic_id`
  and `:partitions`; `nil` stays `nil` ("unchanged").
  """
  @spec build_topic_partitions([map()] | nil) :: [%{topic_id: binary(), partitions: [integer()]}] | nil
  def build_topic_partitions(nil), do: nil

  def build_topic_partitions(topic_partitions) when is_list(topic_partitions) do
    Enum.map(topic_partitions, fn %{topic_id: <<_::128>> = topic_id, partitions: partitions} ->
      %{topic_id: topic_id, partitions: partitions}
    end)
  end
end
