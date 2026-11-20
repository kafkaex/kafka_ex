defmodule KafkaEx.TestSupport.FrontPolicies do
  @moduledoc false

  alias KafkaEx.Client
  alias KafkaEx.Client.Front.Policy
  alias KafkaEx.Client.NodeSelector
  alias KafkaEx.Client.RequestContext
  alias KafkaEx.Support.Retry

  @topic "t"
  @group "g"

  @classes [
    :produce,
    :acks0,
    :fetch,
    :join,
    :sync,
    :heartbeat,
    :kip848_member,
    :kip848_join,
    :commit,
    :offset_fetch,
    :send_node,
    :send_any,
    :find_any
  ]

  def classes, do: @classes

  @spec conn(pos_integer(), term()) :: {binary(), 9092, term()}
  def conn(node, role), do: {"b#{node}", 9092, role}

  @spec policy(atom(), non_neg_integer()) :: Policy.t()
  def policy(:produce, p),
    do:
      ctx(NodeSelector.topic_partition(@topic, p), &Retry.produce_retryable?/1, Client.retry_count(), gate: {@topic, p})

  def policy(:acks0, p), do: Policy.acks0(NodeSelector.topic_partition(@topic, p))

  def policy(:fetch, p),
    do: ctx(NodeSelector.topic_partition_replica(@topic, p), &Retry.data_plane_retryable?/1, Client.retry_count())

  def policy(:join, _p), do: ctx(group(), &Retry.join_group_retryable?/1, Client.coordinator_max_attempts())
  def policy(:sync, _p), do: ctx(group(), &Retry.sync_group_retryable?/1, Client.coordinator_max_attempts())
  def policy(:heartbeat, _p), do: ctx(group(), &Retry.heartbeat_retryable?/1, Client.retry_count())
  def policy(:commit, _p), do: ctx(group(), &Retry.commit_retryable?/1, Client.retry_count())
  def policy(:offset_fetch, _p), do: ctx(group(), &Retry.data_plane_retryable?/1, Client.retry_count())

  def policy(:kip848_member, _p),
    do:
      ctx(group(), &Retry.consumer_group_heartbeat_retryable?/1, Client.retry_count(),
        send_once?: &Retry.consumer_group_heartbeat_send_once?/1,
        rediscover_coordinator?: true
      )

  def policy(:kip848_join, _p),
    do: ctx(group(), &Retry.consumer_group_heartbeat_retryable?/1, Client.retry_count(), rediscover_coordinator?: true)

  def policy(:send_node, p), do: Policy.send_request(NodeSelector.node_id(p + 1))
  def policy(:send_any, _p), do: Policy.send_request(NodeSelector.first_available())

  def policy(:find_any, _p),
    do: ctx(NodeSelector.first_available(), &Retry.data_plane_retryable?/1, Client.retry_count())

  defp group, do: NodeSelector.consumer_group(@group)

  defp ctx(selector, retryable?, max_attempts, opts \\ []) do
    {ctx_opts, policy_opts} = Keyword.split(opts, [:send_once?, :rediscover_coordinator?])

    %RequestContext{request: :model, parser_fn: & &1, node_selector: selector, retryable?: retryable?}
    |> struct(ctx_opts)
    |> Policy.from_context([max_attempts: max_attempts] ++ policy_opts)
  end
end
