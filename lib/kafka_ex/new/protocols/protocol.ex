defmodule KafkaEx.New.Client.Protocol do
  @moduledoc """
  This module is responsible for defining the behaviour of a protocol.
  """
  # ------------------------------------------------------------------------------
  @type api_version :: non_neg_integer
  @type params :: Keyword.t()

  # ------------------------------------------------------------------------------
  @callback build_request(:describe_groups, integer, params) :: term

  # ------------------------------------------------------------------------------
  @type consumer_group :: KafkaEx.New.Structs.ConsumerGroup

  @callback parse_response(:describe_groups, term) ::
              {:ok, [consumer_group]} | {:error, term}
end
