defmodule KafkaEx.New.KafkaExAPI do
  @moduledoc false

  alias KafkaEx.ServerKayrock

  def latest_offset(client, topic, partition) do
    request = %Kayrock.ListOffsets.V1.Request{
      replica_id: -1,
      topics: [
        %{topic: topic, partitions: [%{partition: partition, timestamp: -2}]}
      ]
    }

    {:ok, resp} =
      ServerKayrock.kayrock_call(
        client,
        request,
        {:topic_partition, topic, partition}
      )

    [topic_resp] = resp.responses
    [%{error_code: error_code, offset: offset}] = topic_resp.partition_responses

    case error_code do
      0 -> {:ok, offset}
      _ -> {:error, Kayrock.ErrorCode.code_to_atom(error_code)}
    end
  end
end
