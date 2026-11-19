defmodule KafkaEx.Client.Routing do
  @moduledoc false

  alias KafkaEx.Client.Connection
  alias KafkaEx.Client.NodeSelector

  @metadata_ops [:metadata, :find_coordinator, :api_versions]

  @spec role(NodeSelector.t(), atom()) :: Connection.role()
  def role(_selector, op) when op in @metadata_ops, do: :metadata
  def role(%NodeSelector{strategy: :consumer_group}, _op), do: :coordinator

  def role(%NodeSelector{strategy: strategy}, :fetch) when strategy in [:topic_partition, :topic_partition_replica],
    do: :fetch

  def role(_selector, _op), do: :data
end
