defmodule KafkaEx.New.NodeSelector do
  @moduledoc """
  Defines node selector functions and macros
  """

  alias KafkaEx.New.KafkaExAPI

  defstruct strategy: nil,
            node_id: nil,
            topic: nil,
            partition: nil,
            consumer_group_name: nil

  @type valid_strategy ::
          :node_id
          | :random
          | :first_available
          | :controller
          | :topic_partition
          | :consumer_group
  @type t :: %__MODULE__{
          strategy: valid_strategy,
          node_id: non_neg_integer
        }

  @doc """
  Select a specific node
  """
  @spec node_id(KafkaExAPI.node_id()) :: t
  def node_id(node_id) when is_integer(node_id) do
    %__MODULE__{strategy: :node_id, node_id: node_id}
  end

  @doc """
  Select a random node
  """
  @spec random :: t
  def random, do: %__MODULE__{strategy: :random}

  @doc """
  Select first available node
  """
  @spec first_available :: t
  def first_available, do: %__MODULE__{strategy: :first_available}

  @doc """
  Select the cluster's controller node
  """
  @spec controller :: t
  def controller, do: %__MODULE__{strategy: :controller}

  @doc """
  Select the controller for the given topic and partition
  """
  @spec topic_partition(KafkaExAPI.topic_name(), KafkaExAPI.partition_id()) :: t
  def topic_partition(topic, partition)
      when is_binary(topic) and is_integer(partition) do
    %__MODULE__{
      strategy: :topic_partition,
      topic: topic,
      partition: partition
    }
  end

  @doc """
  Select the controller for the given consumer group
  """
  @spec consumer_group(KafkaExAPI.consumer_group_name()) :: t
  def consumer_group(consumer_group_name) when is_binary(consumer_group_name) do
    %__MODULE__{
      strategy: :consumer_group,
      consumer_group_name: consumer_group_name
    }
  end
end
