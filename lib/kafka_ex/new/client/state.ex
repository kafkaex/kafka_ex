defmodule KafkaEx.New.Client.State do
  @moduledoc false

  # state struct for New.Client

  alias KafkaEx.New.Structs.ClusterMetadata

  defstruct(
    bootstrap_uris: [],
    cluster_metadata: %ClusterMetadata{},
    correlation_id: 0,
    consumer_group_for_auto_commit: nil,
    metadata_update_interval: nil,
    consumer_group_update_interval: nil,
    worker_name: KafkaEx.Server,
    ssl_options: [],
    use_ssl: false,
    api_versions: %{},
    allow_auto_topic_creation: true
  )

  @type t :: %__MODULE__{}

  @default_metadata_update_interval 30_000
  @default_consumer_group_update_interval 30_000

  # initialize static parts of the state from args
  def static_init(args, worker_name) do
    %__MODULE__{
      bootstrap_uris: Keyword.get(args, :uris, []),
      worker_name: worker_name,
      metadata_update_interval:
        Keyword.get(
          args,
          :metadata_update_interval,
          @default_metadata_update_interval
        ),
      consumer_group_update_interval:
        Keyword.get(
          args,
          :consumer_group_update_interval,
          @default_consumer_group_update_interval
        ),
      allow_auto_topic_creation: Keyword.get(args, :allow_auto_topic_creation, true),
      use_ssl: Keyword.get(args, :use_ssl, false),
      ssl_options: Keyword.get(args, :ssl_options, []),
      consumer_group_for_auto_commit: Keyword.get(args, :consumer_group)
    }
  end

  def increment_correlation_id(%__MODULE__{correlation_id: cid} = state) do
    %{state | correlation_id: cid + 1}
  end

  def select_broker(
        %__MODULE__{cluster_metadata: cluster_metadata},
        selector
      ) do
    with {:ok, node_id} <-
           ClusterMetadata.select_node(cluster_metadata, selector),
         broker <- ClusterMetadata.broker_by_node_id(cluster_metadata, node_id) do
      {:ok, broker}
    else
      err -> err
    end
  end

  def update_brokers(
        %__MODULE__{cluster_metadata: cluster_metadata} = state,
        cb
      )
      when is_function(cb, 1) do
    %{
      state
      | cluster_metadata: ClusterMetadata.update_brokers(cluster_metadata, cb)
    }
  end

  def put_consumer_group_coordinator(
        %__MODULE__{cluster_metadata: cluster_metadata} = state,
        consumer_group,
        coordinator_node_id
      ) do
    %{
      state
      | cluster_metadata:
          ClusterMetadata.put_consumer_group_coordinator(
            cluster_metadata,
            consumer_group,
            coordinator_node_id
          )
    }
  end

  def remove_topics(
        %__MODULE__{cluster_metadata: cluster_metadata} = state,
        topics
      ) do
    %{
      state
      | cluster_metadata: ClusterMetadata.remove_topics(cluster_metadata, topics)
    }
  end

  def topics_metadata(
        %__MODULE__{cluster_metadata: cluster_metadata},
        wanted_topics
      ) do
    ClusterMetadata.topics_metadata(cluster_metadata, wanted_topics)
  end

  def brokers(%__MODULE__{cluster_metadata: cluster_metadata}) do
    ClusterMetadata.brokers(cluster_metadata)
  end

  def ingest_api_versions(%__MODULE__{} = state, %{api_versions: api_versions}) do
    api_versions =
      Enum.into(
        api_versions,
        %{},
        fn %{
             api_key: api_key,
             min_version: min_version,
             max_version: max_version
           } ->
          {api_key, {min_version, max_version}}
        end
      )

    %{state | api_versions: api_versions}
  end

  def max_supported_api_version(
        %__MODULE__{api_versions: api_versions},
        api,
        default
      )
      when is_atom(api) do
    api_key = Kayrock.KafkaSchemaMetadata.api_key(api)
    {_, max_kayrock_version} = Kayrock.KafkaSchemaMetadata.version_range(api)

    case Map.get(api_versions, api_key) do
      {_, vsn} -> min(vsn, max_kayrock_version)
      nil -> default
    end
  end
end
