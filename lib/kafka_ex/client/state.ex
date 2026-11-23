defmodule KafkaEx.Client.State do
  @moduledoc false

  # state struct for New.Client

  alias KafkaEx.Client.ConnectionStateMachine
  alias KafkaEx.Client.MetadataStore.Snapshot
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
    transport: KafkaEx.Network.NetworkClient,
    supervisor: nil,
    conns: %{},
    reconnect_backoff: nil,
    connection_opts: [],
    connection_options: %{},
    parked: %{},
    parking: nil,
    store: nil,
    store_opts: [],
    hints: %{},
    deadline: nil,
    dispatch_stamp: nil,
    last_view: %ClusterMetadata{}
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
            :first | {:resumed, {binary(), non_neg_integer(), ConnectionStateMachine.role()}, pid(), term()} | nil,
          store: %{pid: pid(), ref: reference(), tid: Snapshot.t()} | nil,
          store_opts: Keyword.t(),
          deadline: integer() | nil,
          dispatch_stamp: Snapshot.stamp() | nil,
          hints: Snapshot.hints(),
          last_view: ClusterMetadata.t()
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
