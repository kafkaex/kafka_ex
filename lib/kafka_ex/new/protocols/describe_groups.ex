defprotocol KafkaEx.New.Protocols.DescribeGroups do
  @moduledoc """
  This module handles Describe Groups request & response handling & parsing
  """

  defprotocol Request do
    @spec build_request(t(), [KafkaEx.New.KafkaExAPI.consumer_group_name()]) ::
            t()
    @spec build_request(
            t(),
            [KafkaEx.New.KafkaExAPI.consumer_group_name()],
            Keyword.t()
          ) :: t()
    def build_request(request_template, consumer_group_name, opts \\ [])
  end

  defprotocol Response do
    @spec parse_response(t()) ::
            {:ok, [KafkaEx.New.ConsumerGroup.t()]} | {:error, term}
    def parse_response(response)
  end
end
