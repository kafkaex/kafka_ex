defmodule KafkaEx.New.Client.ResponseParser do
  @moduledoc """
  This module is used to parse response from KafkaEx.New.Client.
  It's main decision point which protocol to use for parsing response
  """
  alias KafkaEx.New.Structs.ConsumerGroup

  @protocol Application.compile_env(
              :kafka_ex,
              :protocol,
              KafkaEx.New.Protocols.KayrockProtocol
            )

  @doc """
  Parses response for Describe Groups API
  """
  @spec describe_groups_response(term) ::
          {:ok, [ConsumerGroup.t()]} | {:error, term}
  def describe_groups_response(response) do
    @protocol.parse_response(:describe_groups, response)
  end
end
