defmodule KafkaEx.Cluster.ClusterMetadataTopicIdTest do
  use ExUnit.Case, async: true

  alias KafkaEx.Cluster.ClusterMetadata
  alias KafkaEx.Cluster.Topic

  @id_a <<1::128>>
  @id_b <<2::128>>

  defp metadata do
    %ClusterMetadata{
      topics: %{
        "a" => %Topic{name: "a", topic_id: @id_a},
        "b" => %Topic{name: "b", topic_id: @id_b},
        "legacy" => %Topic{name: "legacy"}
      }
    }
  end

  test "topic_name_by_id/2 resolves known ids" do
    assert ClusterMetadata.topic_name_by_id(metadata(), @id_a) == {:ok, "a"}
    assert ClusterMetadata.topic_name_by_id(metadata(), @id_b) == {:ok, "b"}
  end

  test "topic_name_by_id/2 returns :no_such_topic for unknown ids" do
    assert ClusterMetadata.topic_name_by_id(metadata(), <<9::128>>) == {:error, :no_such_topic}
    assert ClusterMetadata.topic_name_by_id(%ClusterMetadata{}, @id_a) == {:error, :no_such_topic}
  end

  test "topic_ids_by_name/2 returns ids only for known topics with ids" do
    assert ClusterMetadata.topic_ids_by_name(metadata(), ["a", "legacy", "missing"]) == %{"a" => @id_a}
    assert ClusterMetadata.topic_ids_by_name(metadata(), []) == %{}
  end
end
