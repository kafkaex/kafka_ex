defmodule KafkaEx.Support.Types do
  @moduledoc """
  KafkaEx.Types
  This module contains basic types shared between KafkaEx modules
  """

  @typedoc """
  Integer representing error code returned by kafka
  0 means no error, any other value matches some error
  """
  @type error_code :: integer

  @typedoc """
  Topic name UTF-8 Encoded
  """
  @type topic :: String.t()

  @typedoc """
  Integer representing a partition number in topic
  """
  @type partition :: non_neg_integer

  @typedoc """
  Integer representing an offset of a given message
  Unique per topic/partition pair
   - 0+ values is existing offset
   - -1 means offset does not exist
  """
  @type offset :: non_neg_integer | -1

  @typedoc """
  Representation of a Kafka timestamp in elixir as epoch in miliseconds
  """
  @type timestamp :: pos_integer

  @typedoc """
  Representation of a Kafka timestamp request in elixir format
  """
  @type timestamp_request :: DateTime.t() | pos_integer | :earliest | :latest

  @typedoc """
  Consumer group name UTF-8 encoded
  """
  @type consumer_group_name :: String.t()
end
