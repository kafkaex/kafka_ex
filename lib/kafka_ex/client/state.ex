defmodule KafkaEx.Client.State do
  @moduledoc false

  # state struct for New.Client

  alias KafkaEx.Client.ConnectionStateMachine
  alias KafkaEx.Cluster.ClusterMetadata
  alias KafkaEx.Messages.ApiVersions

  @protocol Application.compile_env(:kafka_ex, :protocol, KafkaEx.Protocol.KayrockProtocol)

  defstruct(
    bootstrap_uris: [],
    cluster_metadata: %ClusterMetadata{},
    correlation_id: 0,
    consumer_group_for_auto_commit: nil,
    metadata_update_interval: nil,
    ssl_options: [],
    auth: nil,
    use_ssl: false,
    api_versions: %{},
    allow_auto_topic_creation: true,
    metadata_timer_ref: nil,
    tracked_topics: MapSet.new(),
    metadata_missing: MapSet.new(),
    metadata_missing_logged_at: nil,
    transport: KafkaEx.Network.NetworkClient,
    supervisor: nil,
    conns: %{},
    reconnect_backoff: nil,
    connection_opts: [],
    connection_options: %{},
    parked: %{},
    parking: nil
  )

  @type t :: %__MODULE__{
          bootstrap_uris: [{binary(), pos_integer()}],
          cluster_metadata: ClusterMetadata.t(),
          correlation_id: non_neg_integer(),
          consumer_group_for_auto_commit: binary() | nil,
          metadata_update_interval: pos_integer() | nil,
          ssl_options: Keyword.t(),
          auth: term(),
          use_ssl: boolean(),
          api_versions: map(),
          allow_auto_topic_creation: boolean(),
          metadata_timer_ref: reference() | nil,
          tracked_topics: MapSet.t(),
          metadata_missing: MapSet.t(),
          metadata_missing_logged_at: integer() | nil,
          transport: module(),
          supervisor: pid() | nil,
          conns: %{
            optional({binary(), non_neg_integer(), ConnectionStateMachine.role()}) => %{pid: pid(), ref: reference()}
          },
          reconnect_backoff: :ets.tid() | nil,
          connection_opts: Keyword.t(),
          connection_options: map(),
          parked: %{optional(reference()) => map()},
          parking:
            :first | {:resumed, {binary(), non_neg_integer(), ConnectionStateMachine.role()}, pid(), term()} | nil
        }

  @default_metadata_update_interval 30_000

  def static_init(args) do
    %__MODULE__{
      bootstrap_uris: Keyword.get(args, :uris, []),
      metadata_update_interval: Keyword.get(args, :metadata_update_interval, @default_metadata_update_interval),
      allow_auto_topic_creation: Keyword.get(args, :allow_auto_topic_creation, true),
      use_ssl: Keyword.get(args, :use_ssl, false),
      ssl_options: Keyword.get(args, :ssl_options, []),
      auth: Keyword.get(args, :auth, nil),
      transport: Keyword.get(args, :transport, KafkaEx.Network.NetworkClient),
      connection_opts: Keyword.get(args, :connection, []),
      # Inert unless used: group-coordination calls (offset_commit/fetch,
      # join/sync/leave/heartbeat, describe_groups) and the stream auto-commit
      # path all take the group as an explicit argument — none fall back to
      # this stored value. So defaulting a produce-only client to a real group
      # name never triggers FindCoordinator/JoinGroup/OffsetCommit. Preserve
      # that invariant if you ever add an implicit default off this field.
      consumer_group_for_auto_commit: Keyword.get(args, :consumer_group)
    }
  end

  def increment_correlation_id(%__MODULE__{correlation_id: cid} = state) do
    %{state | correlation_id: cid + 1}
  end

  def select_broker(%__MODULE__{cluster_metadata: cluster_metadata}, selector) do
    with {:ok, node_id} <- ClusterMetadata.select_node(cluster_metadata, selector),
         broker when not is_nil(broker) <- ClusterMetadata.broker_by_node_id(cluster_metadata, node_id) do
      {:ok, broker}
    else
      nil -> {:error, :broker_not_found}
      err -> err
    end
  end

  def update_brokers(%__MODULE__{cluster_metadata: cluster_metadata} = state, cb) when is_function(cb, 1) do
    %{state | cluster_metadata: ClusterMetadata.update_brokers(cluster_metadata, cb)}
  end

  def select_broker_entry(%__MODULE__{cluster_metadata: cluster_metadata}, selector) do
    with {:ok, key} <- ClusterMetadata.select_node(cluster_metadata, selector),
         broker when not is_nil(broker) <- ClusterMetadata.broker_by_node_id(cluster_metadata, key) do
      {:ok, key, broker}
    else
      nil -> {:error, :broker_not_found}
      err -> err
    end
  end

  def put_consumer_group_coordinator(%__MODULE__{cluster_metadata: cluster_metadata} = state, group, node_id) do
    new_metadata = ClusterMetadata.put_consumer_group_coordinator(cluster_metadata, group, node_id)
    %{state | cluster_metadata: new_metadata}
  end

  def drop_consumer_group_coordinator(%__MODULE__{cluster_metadata: cluster_metadata} = state, group) do
    new_metadata = ClusterMetadata.drop_consumer_group_coordinator(cluster_metadata, group)
    %{state | cluster_metadata: new_metadata}
  end

  def remove_topics(%__MODULE__{cluster_metadata: cluster_metadata} = state, topics) do
    %{state | cluster_metadata: ClusterMetadata.remove_topics(cluster_metadata, topics)}
  end

  def topics_metadata(%__MODULE__{cluster_metadata: cluster_metadata}, wanted_topics) do
    ClusterMetadata.topics_metadata(cluster_metadata, wanted_topics)
  end

  def brokers(%__MODULE__{cluster_metadata: cluster_metadata}) do
    ClusterMetadata.brokers(cluster_metadata)
  end

  @spec api_version_ranges(ApiVersions.t()) :: %{non_neg_integer() => {non_neg_integer(), non_neg_integer()}}
  def api_version_ranges(%ApiVersions{api_versions: api_versions_map}) do
    Map.new(api_versions_map, fn {api_key, %{min_version: min_version, max_version: max_version}} ->
      {api_key, {min_version, max_version}}
    end)
  end

  @doc """
  Returns max supported api version for request based on cached values in state.

  Returns `{:ok, version}` when the broker supports the API, or
  `{:error, :api_not_supported_by_broker}` when the broker does not report the API.
  """
  def max_supported_api_version(%__MODULE__{api_versions: api_versions}, api) when is_atom(api) do
    api_key = @protocol.api_key(api)
    max_kayrock_version = @protocol.max_supported_version(api)

    case Map.get(api_versions, api_key) do
      {_, vsn} -> {:ok, min(vsn, max_kayrock_version)}
      nil -> {:error, :api_not_supported_by_broker}
    end
  end

  @doc """
  Deprecated 3-arity version kept for backward compatibility.
  Will be removed when internal callers are migrated.
  """
  @deprecated "Use max_supported_api_version/2 instead. Will be removed in v2.0."
  def max_supported_api_version(%__MODULE__{} = state, api, default) when is_atom(api) do
    case max_supported_api_version(state, api) do
      {:ok, version} -> version
      {:error, :api_not_supported_by_broker} -> default
    end
  end
end
