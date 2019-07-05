defmodule KafkaEx.New.Topic do
  @moduledoc false

  defstruct name: nil, partition_leaders: %{}

  def from_topic_metadata(%{topic: name, partition_metadata: partition_metadata}) do
    partition_leaders =
      Enum.into(
        partition_metadata,
        %{},
        fn %{error_code: 0, leader: leader, partition: partition_id} ->
          {partition_id, leader}
        end
      )

    %__MODULE__{name: name, partition_leaders: partition_leaders}
  end
end
