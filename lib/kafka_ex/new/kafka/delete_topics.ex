defmodule KafkaEx.New.Kafka.DeleteTopics do
  @moduledoc """
  Represents the result of a DeleteTopics API call.

  This struct contains the results for all topics that were requested to be deleted,
  including any errors that occurred during deletion.

  ## Fields

  - `topic_results` - List of `TopicResult` structs, one for each topic
  - `throttle_time_ms` - Time in milliseconds the request was throttled (V1+)

  ## Example

      %DeleteTopics{
        topic_results: [
          %TopicResult{topic: "my-topic", error: :no_error},
          %TopicResult{topic: "unknown-topic", error: :unknown_topic_or_partition}
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
  Builds a DeleteTopics struct from response data.
  """
  @spec build(Keyword.t()) :: t()
  def build(opts \\ []) do
    %__MODULE__{
      topic_results: Keyword.get(opts, :topic_results, []),
      throttle_time_ms: Keyword.get(opts, :throttle_time_ms)
    }
  end

  @doc """
  Returns true if all topics were deleted successfully.
  """
  @spec success?(t()) :: boolean()
  def success?(%__MODULE__{topic_results: results}) do
    Enum.all?(results, &TopicResult.success?/1)
  end

  @doc """
  Returns the list of topics that failed to be deleted.
  """
  @spec failed_topics(t()) :: [TopicResult.t()]
  def failed_topics(%__MODULE__{topic_results: results}) do
    Enum.reject(results, &TopicResult.success?/1)
  end

  @doc """
  Returns the list of topics that were deleted successfully.
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
    Represents the result of deleting a single topic.

    ## Fields

    - `topic` - Name of the topic
    - `error` - Error code as atom (:no_error on success)
    """

    defstruct [:topic, :error]

    @type t :: %__MODULE__{
            topic: String.t(),
            error: atom()
          }

    @doc """
    Builds a TopicResult struct.
    """
    @spec build(Keyword.t()) :: t()
    def build(opts) do
      %__MODULE__{
        topic: Keyword.fetch!(opts, :topic),
        error: Keyword.get(opts, :error, :no_error)
      }
    end

    @doc """
    Returns true if the topic was deleted successfully.
    """
    @spec success?(t()) :: boolean()
    def success?(%__MODULE__{error: :no_error}), do: true
    def success?(%__MODULE__{}), do: false
  end
end
