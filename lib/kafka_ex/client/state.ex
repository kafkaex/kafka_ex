defmodule KafkaEx.Client.State do
  @moduledoc false

  # state struct for New.Client

  alias KafkaEx.Cluster.Broker
  alias KafkaEx.Cluster.ClusterMetadata
  alias KafkaEx.Messages.ApiVersions
  alias KafkaEx.Network.Socket

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
    connections: %{},
    transport: KafkaEx.Network.NetworkClient
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
          connections: %{optional(term()) => connection()},
          transport: module()
        }

  @type connection :: %{
          node_id: non_neg_integer() | nil,
          host: binary(),
          port: non_neg_integer(),
          socket: Socket.t()
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

  def select_broker_entry(%__MODULE__{cluster_metadata: cluster_metadata} = state, selector) do
    with {:ok, key} <- ClusterMetadata.select_node(cluster_metadata, selector),
         broker when not is_nil(broker) <- ClusterMetadata.broker_by_node_id(cluster_metadata, key) do
      {:ok, key, broker_with_socket(state, key, broker)}
    else
      nil -> {:error, :broker_not_found}
      err -> err
    end
  end

  def broker_entries(%__MODULE__{cluster_metadata: %ClusterMetadata{brokers: brokers}} = state) do
    Enum.map(brokers, fn {key, broker} -> {key, broker_with_socket(state, key, broker)} end)
  end

  def connections(%__MODULE__{connections: connections}), do: connections

  def connection_socket(%__MODULE__{connections: connections}, key) do
    case Map.get(connections, key) do
      %{socket: socket} -> socket
      nil -> nil
    end
  end

  def put_connection(%__MODULE__{} = state, key, _address, nil), do: drop_connection(state, key)

  def put_connection(%__MODULE__{connections: connections} = state, key, %{host: host, port: port} = address, socket) do
    entry = %{node_id: Map.get(address, :node_id), host: host, port: port, socket: socket}
    mirror(%{state | connections: Map.put(connections, key, entry)}, key)
  end

  def drop_connection(%__MODULE__{connections: connections} = state, key) do
    mirror(%{state | connections: Map.delete(connections, key)}, key)
  end

  def find_connections_by_raw_socket(%__MODULE__{connections: connections}, raw) do
    for {key, %{socket: %Socket{socket: ^raw}} = entry} <- connections, do: {key, entry}
  end

  # Like merge_brokers/2: every broker at an address gets the first old entry there, in map order.
  def reconcile_connections(%__MODULE__{connections: old, cluster_metadata: %ClusterMetadata{brokers: brokers}} = state) do
    by_address = Enum.reduce(old, %{}, fn {_key, entry}, acc -> Map.put_new(acc, {entry.host, entry.port}, entry) end)

    kept =
      for {key, broker} <- brokers,
          {:ok, entry} <- [Map.fetch(by_address, {broker.host, broker.port})],
          into: %{},
          do: {key, %{entry | node_id: broker.node_id}}

    kept_sockets = MapSet.new(kept, fn {_, entry} -> entry.socket end)
    stale = for {_, entry} <- old, not MapSet.member?(kept_sockets, entry.socket), do: entry

    {sync_mirror(%{state | connections: kept}), stale}
  end

  defp sync_mirror(%__MODULE__{cluster_metadata: %ClusterMetadata{brokers: brokers} = cluster_metadata} = state) do
    mirrored = Map.new(brokers, fn {key, broker} -> {key, broker_with_socket(state, key, broker)} end)
    %{state | cluster_metadata: %{cluster_metadata | brokers: mirrored}}
  end

  defp mirror(%__MODULE__{cluster_metadata: %ClusterMetadata{brokers: brokers} = cluster_metadata} = state, key) do
    case Map.fetch(brokers, key) do
      {:ok, broker} ->
        brokers = Map.put(brokers, key, broker_with_socket(state, key, broker))
        %{state | cluster_metadata: %{cluster_metadata | brokers: brokers}}

      :error ->
        state
    end
  end

  defp broker_with_socket(%__MODULE__{connections: connections}, key, %Broker{host: host, port: port} = broker) do
    case Map.get(connections, key) do
      %{host: ^host, port: ^port, socket: socket} -> %{broker | socket: socket}
      _ -> %{broker | socket: nil}
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

  def ingest_api_versions(%__MODULE__{} = state, %ApiVersions{api_versions: api_versions_map}) do
    api_versions =
      Enum.into(api_versions_map, %{}, fn {api_key, %{min_version: min_version, max_version: max_version}} ->
        {api_key, {min_version, max_version}}
      end)

    %{state | api_versions: api_versions}
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
