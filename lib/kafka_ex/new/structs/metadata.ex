defmodule KafkaEx.New.Structs.Metadata do
  @moduledoc """
  Represents a Metadata request and response for the Kafka Metadata API.

  The Metadata API is used to fetch information about topics, partitions, brokers,
  and the controller from the Kafka cluster.
  """

  alias KafkaEx.New.Structs.ClusterMetadata

  @type topics :: nil | [String.t()]
  @type api_version :: 0 | 1 | 2

  defmodule Request do
    @moduledoc """
    Represents a Metadata request.
    """

    defstruct topics: nil, allow_auto_topic_creation: false

    @type t :: %__MODULE__{
            topics: nil | [String.t()],
            allow_auto_topic_creation: boolean()
          }
  end

  defmodule Response do
    @moduledoc """
    Represents a Metadata response.

    For most use cases, the `cluster_metadata` field provides all necessary
    information through the `ClusterMetadata` struct.
    """

    defstruct cluster_metadata: nil, throttle_time_ms: nil

    @type t :: %__MODULE__{
            cluster_metadata: ClusterMetadata.t(),
            throttle_time_ms: non_neg_integer() | nil
          }
  end

  @doc """
  Creates a metadata request for all topics.
  """
  @spec request() :: Request.t()
  def request do
    %Request{topics: nil, allow_auto_topic_creation: false}
  end

  @spec request(nil | [String.t()]) :: Request.t()
  @doc """
  Creates a metadata request for specific topics.
  Pass `nil` or an empty list to request metadata for all topics.
  """
  def request(nil), do: %Request{topics: nil, allow_auto_topic_creation: false}
  def request([]), do: %Request{topics: nil, allow_auto_topic_creation: false}

  def request([{key, _} | _] = opts) when is_atom(key) do
    %Request{
      topics: Keyword.get(opts, :topics),
      allow_auto_topic_creation: Keyword.get(opts, :allow_auto_topic_creation, false)
    }
  end

  def request(topics) when is_list(topics) do
    %Request{topics: topics, allow_auto_topic_creation: false}
  end

  @doc """
  Builds a response struct from cluster metadata.
  """
  @spec response(ClusterMetadata.t(), Keyword.t()) :: Response.t()
  def response(cluster_metadata, opts \\ []) do
    %Response{
      cluster_metadata: cluster_metadata,
      throttle_time_ms: Keyword.get(opts, :throttle_time_ms)
    }
  end

  @doc """
  Checks if a topic exists in the metadata response.
  """
  @spec topic_exists?(Response.t(), String.t()) :: boolean()
  def topic_exists?(%Response{cluster_metadata: cluster_metadata}, topic) do
    topic in ClusterMetadata.known_topics(cluster_metadata)
  end

  @doc """
  Gets the controller broker from the metadata response.
  """
  @spec controller(Response.t()) :: {:ok, KafkaEx.New.Structs.Broker.t()} | {:error, :no_controller}
  def controller(%Response{cluster_metadata: %ClusterMetadata{controller_id: nil}}) do
    {:error, :no_controller}
  end

  def controller(%Response{
        cluster_metadata: %ClusterMetadata{controller_id: controller_id} = cluster_metadata
      }) do
    case ClusterMetadata.broker_by_node_id(cluster_metadata, controller_id) do
      nil -> {:error, :no_controller}
      broker -> {:ok, broker}
    end
  end

  @doc """
  Gets all brokers from the metadata response.
  """
  @spec brokers(Response.t()) :: [KafkaEx.New.Structs.Broker.t()]
  def brokers(%Response{cluster_metadata: cluster_metadata}) do
    ClusterMetadata.brokers(cluster_metadata)
  end

  @doc """
  Gets metadata for a specific topic from the response.
  Returns `{:ok, topic}` if the topic exists, otherwise `{:error, :no_such_topic}`.
  """
  @spec topic(Response.t(), String.t()) :: {:ok, KafkaEx.New.Structs.Topic.t()} | {:error, :no_such_topic}
  def topic(%Response{cluster_metadata: cluster_metadata}, topic_name) do
    case ClusterMetadata.topics_metadata(cluster_metadata, [topic_name]) do
      [topic] -> {:ok, topic}
      [] -> {:error, :no_such_topic}
    end
  end

  @doc """
  Gets metadata for multiple topics from the response.
  """
  @spec topics(Response.t(), [String.t()]) :: [KafkaEx.New.Structs.Topic.t()]
  def topics(%Response{cluster_metadata: cluster_metadata}, topic_names) do
    ClusterMetadata.topics_metadata(cluster_metadata, topic_names)
  end

  @doc """
  Gets all known topics from the metadata response.
  """
  @spec all_topics(Response.t()) :: [KafkaEx.New.Structs.Topic.t()]
  def all_topics(%Response{cluster_metadata: cluster_metadata}) do
    topic_names = ClusterMetadata.known_topics(cluster_metadata)
    ClusterMetadata.topics_metadata(cluster_metadata, topic_names)
  end
end
