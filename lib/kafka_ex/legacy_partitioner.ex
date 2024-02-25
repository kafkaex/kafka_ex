defmodule KafkaEx.LegacyPartitioner do
  @moduledoc """
  Legacy default partitioner implementation.

  This "legacy partitioner" used to be the default partitioner for KafkaEx
  when a message key is set and a partition isn't. This was intended to match
  the behaviour of the default Java client, however there was some small
  differences. This legacy partitioner keeps the previous behaviour, and the
  current default partitioner now correctly matches the Java client.
  """
  use KafkaEx.Partitioner
  alias KafkaEx.Partitioner
  alias KafkaEx.Protocol.Produce.Request, as: ProduceRequest
  alias KafkaEx.Protocol.Metadata.Response, as: MetadataResponse
  alias KafkaEx.Utils.Murmur, as: Murmur
  require Logger

  @spec assign_partition(request :: ProduceRequest.t(), metadata :: MetadataResponse.t()) ::
          ProduceRequest.t()
  def assign_partition(%ProduceRequest{partition: partition} = request, _)
      when is_number(partition) do
    request
  end

  # credo:disable-for-lines:50 Credo.Check.Design.DuplicatedCode
  def assign_partition(%ProduceRequest{partition: nil} = request, metadata) do
    case Partitioner.get_key(request) do
      {:ok, nil} ->
        assign_partition_randomly(request, metadata)

      {:ok, key} ->
        assign_partition_with_key(request, metadata, key)

      {:error, reason} ->
        Logger.warning("#{__MODULE__}: couldn't assign partition due to #{inspect(reason)}")

        assign_partition_randomly(request, metadata)
    end
  end

  defp assign_partition_randomly(%ProduceRequest{topic: topic} = request, metadata) do
    partition_id =
      case MetadataResponse.partitions_for_topic(metadata, topic) do
        [] -> 0
        list -> Enum.random(list)
      end

    %{request | partition: partition_id}
  end

  defp assign_partition_with_key(
         %ProduceRequest{topic: topic} = request,
         metadata,
         key
       ) do
    hash = Murmur.umurmur2_legacy(key)

    partitions_count = metadata |> MetadataResponse.partitions_for_topic(topic) |> length()

    partition_id = rem(hash, partitions_count)
    %{request | partition: partition_id}
  end
end
