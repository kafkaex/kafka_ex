defmodule KafkaEx.Types do
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
  """
  @type offset :: non_neg_integer

  @typedoc """
  Integer representing a kafka timestamp value.
  It's a number of milliseconds since Unix epoch.
  """
  @type timestamp :: integer

  @typedoc """
  Consumer group name UTF-8 encoded
  """
  @type consumer_group_name :: String.t()
end
