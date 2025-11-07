defmodule KafkaEx.New.Structs.Metadata do
  @moduledoc """
  Represents a Metadata request and response for the Kafka Metadata API.

  The Metadata API is used to fetch information about topics, partitions, brokers,
  and the controller from the Kafka cluster.

  ## Request

  Use `request/1` or `request/2` to build a metadata request:

      # Request all topics
      Metadata.request()
      Metadata.request(nil)

      # Request specific topics
      Metadata.request(["topic1", "topic2"])

  ## Response

  The response is typically returned as a `ClusterMetadata` struct which provides
  comprehensive information about the cluster state. See
  `KafkaEx.New.Structs.ClusterMetadata` for details.

  ## API Versions

  - **V0**: Basic metadata (topics, partitions, brokers)
  - **V1**: Adds controller_id, is_internal flag, broker rack
  - **V2**: Adds cluster_id (if supported by Kayrock)

  The default API version is V1, which provides controller information and
  internal topic filtering.
  """

  alias KafkaEx.New.Structs.ClusterMetadata

  @type topics :: nil | [String.t()]
  @type api_version :: 0 | 1 | 2

  defmodule Request do
    @moduledoc """
    Represents a Metadata request.

    ## Fields

    - `topics` - List of topics to fetch metadata for, or `nil` for all topics
    - `allow_auto_topic_creation` - Whether to auto-create topics (V4+, future use)
    """

    defstruct topics: nil,
              allow_auto_topic_creation: false

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

    ## Fields

    - `cluster_metadata` - Parsed cluster state including brokers and topics
    - `throttle_time_ms` - Request throttle time (V1+, may be nil)
    """

    defstruct cluster_metadata: nil,
              throttle_time_ms: nil

    @type t :: %__MODULE__{
            cluster_metadata: ClusterMetadata.t(),
            throttle_time_ms: non_neg_integer() | nil
          }
  end

  @doc """
  Creates a metadata request for all topics.

  ## Examples

      iex> Metadata.request()
      %Metadata.Request{topics: nil, allow_auto_topic_creation: false}
  """
  @spec request() :: Request.t()
  def request do
    %Request{topics: nil, allow_auto_topic_creation: false}
  end

  @spec request(nil | [String.t()]) :: Request.t()
  @doc """
  Creates a metadata request for specific topics.

  Pass `nil` or an empty list to request metadata for all topics.

  ## Examples

      # Request specific topics
      iex> Metadata.request(["topic1", "topic2"])
      %Metadata.Request{topics: ["topic1", "topic2"], allow_auto_topic_creation: false}

      # Request all topics
      iex> Metadata.request(nil)
      %Metadata.Request{topics: nil, allow_auto_topic_creation: false}

      iex> Metadata.request([])
      %Metadata.Request{topics: nil, allow_auto_topic_creation: false}

  Creates a metadata request with options.

  ## Options

  - `:topics` - List of topics or `nil` for all topics (default: `nil`)
  - `:allow_auto_topic_creation` - Auto-create topics if they don't exist (default: `false`)

  ## Examples

      iex> Metadata.request(topics: ["topic1"], allow_auto_topic_creation: true)
      %Metadata.Request{topics: ["topic1"], allow_auto_topic_creation: true}
  """
  def request(nil), do: %Request{topics: nil, allow_auto_topic_creation: false}
  def request([]), do: %Request{topics: nil, allow_auto_topic_creation: false}

  # Keyword list (options)
  def request([{key, _} | _] = opts) when is_atom(key) do
    %Request{
      topics: Keyword.get(opts, :topics),
      allow_auto_topic_creation: Keyword.get(opts, :allow_auto_topic_creation, false)
    }
  end

  # Regular list of topics (strings)
  def request(topics) when is_list(topics) do
    %Request{topics: topics, allow_auto_topic_creation: false}
  end

  @spec response(ClusterMetadata.t(), Keyword.t()) :: Response.t()
  @doc """
  Builds a response struct from cluster metadata.
  """
  def response(cluster_metadata, opts \\ []) do
    %Response{
      cluster_metadata: cluster_metadata,
      throttle_time_ms: Keyword.get(opts, :throttle_time_ms)
    }
  end

  @spec topic_exists?(Response.t(), String.t()) :: boolean()
  @doc """
  Checks if a topic exists in the metadata response.
  """
  def topic_exists?(%Response{cluster_metadata: cluster_metadata}, topic) do
    topic in ClusterMetadata.known_topics(cluster_metadata)
  end

  @spec controller(Response.t()) :: {:ok, KafkaEx.New.Structs.Broker.t()} | {:error, :no_controller}
  @doc """
  Gets the controller broker from the metadata response.

  Returns `{:ok, broker}` if a controller is identified, otherwise `{:error, :no_controller}`.
  """
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

  @spec brokers(Response.t()) :: [KafkaEx.New.Structs.Broker.t()]
  @doc """
  Gets all brokers from the metadata response.
  """
  def brokers(%Response{cluster_metadata: cluster_metadata}) do
    ClusterMetadata.brokers(cluster_metadata)
  end

  @spec topic(Response.t(), String.t()) ::
          {:ok, KafkaEx.New.Structs.Topic.t()} | {:error, :no_such_topic}
  @doc """
  Gets metadata for a specific topic from the response.

  Returns `{:ok, topic}` if the topic exists, otherwise `{:error, :no_such_topic}`.
  """
  def topic(%Response{cluster_metadata: cluster_metadata}, topic_name) do
    case ClusterMetadata.topics_metadata(cluster_metadata, [topic_name]) do
      [topic] -> {:ok, topic}
      [] -> {:error, :no_such_topic}
    end
  end

  @spec topics(Response.t(), [String.t()]) :: [KafkaEx.New.Structs.Topic.t()]
  @doc """
  Gets metadata for multiple topics from the response.
  """
  def topics(%Response{cluster_metadata: cluster_metadata}, topic_names) do
    ClusterMetadata.topics_metadata(cluster_metadata, topic_names)
  end

  @spec all_topics(Response.t()) :: [KafkaEx.New.Structs.Topic.t()]
  @doc """
  Gets all known topics from the metadata response.
  """
  def all_topics(%Response{cluster_metadata: cluster_metadata}) do
    topic_names = ClusterMetadata.known_topics(cluster_metadata)
    ClusterMetadata.topics_metadata(cluster_metadata, topic_names)
  end
end
