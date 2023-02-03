defprotocol KafkaEx.New.Protocols.ListOffsets do
  @moduledoc """
  This module handles ListOffsets request & response handling & parsing
  """

  defprotocol Request do
    @type topic_partitions :: [
            {KafkaEx.New.KafkaExAPI.topic_name(),
             [KafkaEx.New.KafkaExAPI.partition_id()]}
          ]

    @spec build_request(t(), topic_partitions) :: t()
    @spec build_request(t(), topic_partitions, Keyword.t()) :: t()
    def build_request(request_template, topic_partitions, opts \\ [])
  end

  defprotocol Response do
    @spec get_partition_offset(
            t,
            KafkaEx.New.KafkaExAPI.topic_name(),
            KafkaEx.New.KafkaExAPI.partition_id()
          ) :: {:ok, KafkaEx.New.KafkaExAPI.offset()} | {:error, atom}
    def get_partition_offset(response, topic_name, partition_id)
  end
end
