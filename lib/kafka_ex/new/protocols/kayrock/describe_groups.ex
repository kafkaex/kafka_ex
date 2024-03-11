defprotocol KafkaEx.New.Protocols.Kayrock.DescribeGroups do
  @moduledoc """
  This module handles Describe Groups request & response parsing.
  Request is built using Kayrock protocol, response is parsed to
  native KafkaEx structs.
  """

  defprotocol Request do
    @moduledoc """
    This protocol is used to build Describe Groups request
    """
    @spec build_request(t(), [binary]) :: t()
    def build_request(request, consumer_group_names)
  end

  defprotocol Response do
    @moduledoc """
    This protocol is used to parse Describe Groups response
    """
    alias KafkaEx.New.Structs.ConsumerGroup

    @spec parse_response(t()) :: {:ok, [ConsumerGroup.t()]} | {:error, term}
    def parse_response(response)
  end
end
