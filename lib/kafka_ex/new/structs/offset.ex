defmodule KafkaEx.New.Structs.Offset do
  @moduledoc """
  This module represents Offset value coming from Kafka
  """
  defstruct [:topic, :partition, :offset, :timestamp]

  @type topic :: KafkaEx.Types.topic()
  @type partition :: KafkaEx.Types.partition()
  @type offset :: KafkaEx.Types.offset()
  @type timestamp :: KafkaEx.Types.timestamp()

  @type t :: %__MODULE__{
          topic: topic(),
          partition: partition(),
          offset: offset(),
          timestamp: timestamp() | nil
        }

  @spec from_list_offset(topic, partition, offset) :: __MODULE__.t()
  def from_list_offset(topic, partition, offset) do
    %__MODULE__{
      topic: topic,
      partition: partition,
      offset: offset,
      timestamp: nil
    }
  end

  @spec from_list_offset(topic, partition, offset, timestamp) :: __MODULE__.t()
  def from_list_offset(topic, partition, offset, timestamp) do
    %__MODULE__{
      topic: topic,
      partition: partition,
      offset: offset,
      timestamp: timestamp
    }
  end
end
