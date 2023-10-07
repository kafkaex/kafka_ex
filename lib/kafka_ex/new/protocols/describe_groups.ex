defmodule KafkaEx.New.Protocols.DescribeGroups do
  @moduledoc """
  This module handles Describe Groups request & response handling & parsing
  """

  defprotocol Response do
    @moduledoc """
    This protocol is used to parse Describe Groups response
    """
    alias KafkaEx.New.Structs.ConsumerGroup

    @spec parse_response(t()) :: {:ok, [ConsumerGroup.t()]} | {:error, term}
    def parse_response(response)
  end
end
