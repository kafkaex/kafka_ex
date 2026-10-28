defmodule KafkaEx.Protocol.Kayrock.ConsumerGroupHeartbeat.ResponseHelpers do
  @moduledoc """
  Shared helper functions for parsing ConsumerGroupHeartbeat responses (V0-V1).

  V0 and V1 responses are schema-identical.
  """

  alias KafkaEx.Client.Error
  alias KafkaEx.Messages.ConsumerGroupHeartbeat
  alias KafkaEx.Messages.TopicPartitions
  alias KafkaEx.Protocol.Kayrock.ResponseHelpers
  alias Kayrock.ErrorCode

  @doc """
  Parses a V0+ response into `{:ok, %ConsumerGroupHeartbeat{}}` or `{:error, %Error{}}`.

  A `nil` assignment means "unchanged" and stays `nil`.
  """
  @spec parse_response(map()) :: {:ok, ConsumerGroupHeartbeat.t()} | {:error, Error.t()}
  def parse_response(response) do
    case ErrorCode.code_to_atom(response.error_code) do
      :no_error ->
        {:ok,
         ConsumerGroupHeartbeat.build(
           member_id: response.member_id,
           member_epoch: response.member_epoch,
           heartbeat_interval_ms: response.heartbeat_interval_ms,
           assignment: build_assignment(response.assignment),
           throttle_time_ms: response.throttle_time_ms
         )}

      error_atom ->
        {:error, Error.build(error_atom, error_metadata(response))}
    end
  end

  @doc """
  Fills in each assignment entry's `topic_name` using `resolver`
  (`topic_id -> {:ok, name} | {:error, _}`); entries the resolver does not know
  keep `topic_name: nil`. Results without an assignment are returned unchanged.
  """
  @spec put_topic_names({:ok, ConsumerGroupHeartbeat.t()} | {:error, term()}, ResponseHelpers.topic_name_resolver()) ::
          {:ok, ConsumerGroupHeartbeat.t()} | {:error, term()}
  def put_topic_names({:ok, %ConsumerGroupHeartbeat{assignment: assignment} = result}, resolver)
      when is_list(assignment) do
    resolved =
      Enum.map(assignment, fn %TopicPartitions{topic_id: topic_id} = topic_partitions ->
        case resolver.(topic_id) do
          {:ok, name} -> %{topic_partitions | topic_name: name}
          _ -> topic_partitions
        end
      end)

    {:ok, %{result | assignment: resolved}}
  end

  def put_topic_names(result, _resolver), do: result

  defp build_assignment(nil), do: nil

  defp build_assignment(%{topic_partitions: topic_partitions}) do
    Enum.map(topic_partitions || [], &TopicPartitions.build/1)
  end

  defp error_metadata(response) do
    case Map.get(response, :error_message) do
      nil -> %{}
      message -> %{error_message: message}
    end
  end
end
