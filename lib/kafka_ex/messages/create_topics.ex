defmodule KafkaEx.Messages.CreateTopics do
  @moduledoc """
  Represents the result of a CreateTopics API call.

  This struct contains the results for all topics that were requested to be created,
  including any errors that occurred during creation.

  ## Fields

  - `topic_results` - List of `TopicResult` structs, one for each topic
  - `throttle_time_ms` - Time in milliseconds the request was throttled (V2+)

  ## Example

      %CreateTopics{
        topic_results: [
          %TopicResult{topic: "my-topic", error: :no_error, error_message: nil},
          %TopicResult{topic: "existing-topic", error: :topic_already_exists, error_message: "Topic already exists"}
        ],
        throttle_time_ms: 0
      }
  """

  alias __MODULE__.TopicResult

  defstruct [:topic_results, :throttle_time_ms]

  @type t :: %__MODULE__{
          topic_results: [TopicResult.t()],
          throttle_time_ms: non_neg_integer() | nil
        }

  @doc """
  Builds a CreateTopics struct from response data.
  """
  @spec build(Keyword.t()) :: t()
  def build(opts \\ []) do
    %__MODULE__{
      topic_results: Keyword.get(opts, :topic_results, []),
      throttle_time_ms: Keyword.get(opts, :throttle_time_ms)
    }
  end

  @doc """
  Returns true if all topics were created successfully.
  """
  @spec success?(t()) :: boolean()
  def success?(%__MODULE__{topic_results: results}) do
    Enum.all?(results, &TopicResult.success?/1)
  end

  @doc """
  Returns the list of topics that failed to be created.
  """
  @spec failed_topics(t()) :: [TopicResult.t()]
  def failed_topics(%__MODULE__{topic_results: results}) do
    Enum.reject(results, &TopicResult.success?/1)
  end

  @doc """
  Returns the list of topics that were created successfully.
  """
  @spec successful_topics(t()) :: [TopicResult.t()]
  def successful_topics(%__MODULE__{topic_results: results}) do
    Enum.filter(results, &TopicResult.success?/1)
  end

  @doc """
  Returns the result for a specific topic.
  """
  @spec get_topic_result(t(), String.t()) :: TopicResult.t() | nil
  def get_topic_result(%__MODULE__{topic_results: results}, topic_name) do
    Enum.find(results, fn r -> r.topic == topic_name end)
  end

  defmodule TopicResult do
    @moduledoc """
    Represents the result of creating a single topic.

    ## Fields

    - `topic` - Name of the topic
    - `error` - Error code as atom (:no_error on success)
    - `error_message` - Human-readable error message (V1+, nil for V0)
    - `num_partitions` - Number of partitions created (V5+, nil for V0-V4)
    - `replication_factor` - Replication factor (V5+, nil for V0-V4)
    - `configs` - List of topic config maps (V5+, nil for V0-V4).
      Each config map has: `:name`, `:value`, `:read_only`, `:config_source`, `:is_sensitive`
    """

    defstruct [:topic, :error, :error_message, :num_partitions, :replication_factor, :configs]

    @type config_entry :: %{
            name: String.t(),
            value: String.t() | nil,
            read_only: boolean(),
            config_source: integer(),
            is_sensitive: boolean()
          }

    @type t :: %__MODULE__{
            topic: String.t(),
            error: atom(),
            error_message: String.t() | nil,
            num_partitions: integer() | nil,
            replication_factor: integer() | nil,
            configs: [config_entry()] | nil
          }

    @doc """
    Builds a TopicResult struct.
    """
    @spec build(Keyword.t()) :: t()
    def build(opts) do
      %__MODULE__{
        topic: Keyword.fetch!(opts, :topic),
        error: Keyword.get(opts, :error, :no_error),
        error_message: Keyword.get(opts, :error_message),
        num_partitions: Keyword.get(opts, :num_partitions),
        replication_factor: Keyword.get(opts, :replication_factor),
        configs: Keyword.get(opts, :configs)
      }
    end

    @doc """
    Returns true if the topic was created successfully.
    """
    @spec success?(t()) :: boolean()
    def success?(%__MODULE__{error: :no_error}), do: true
    def success?(%__MODULE__{}), do: false
  end
end
