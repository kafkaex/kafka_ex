defmodule KafkaEx.Client.MetadataLogTest do
  use ExUnit.Case, async: true

  alias KafkaEx.Client.MetadataLog
  alias KafkaEx.Cluster.ClusterMetadata

  defp cluster(topic_names) do
    %ClusterMetadata{topics: Map.new(topic_names, fn t -> {t, %{}} end)}
  end

  describe "missing_topics/2" do
    test "returns requested topics not present in the response" do
      cm = cluster(["a", "b"])
      assert MetadataLog.missing_topics(["a", "b"], cm) == []
      assert MetadataLog.missing_topics(["a", "c"], cm) == ["c"]
    end

    test "empty request is never missing (brokers-only / all-topics refresh)" do
      assert MetadataLog.missing_topics([], cluster([])) == []
    end
  end

  describe "should_log?/5" do
    setup do
      %{hb: 900_000}
    end

    test "logs the first time (no prior log)", %{hb: hb} do
      assert MetadataLog.should_log?(MapSet.new(["a"]), MapSet.new(["a"]), nil, 1_000, hb)
    end

    test "silent when the missing set is unchanged within the heartbeat window", %{hb: hb} do
      set = MapSet.new(["a"])
      refute MetadataLog.should_log?(set, set, 1_000, 1_500, hb)
    end

    test "logs when the missing set changes", %{hb: hb} do
      refute_set = MapSet.new(["a"])
      assert MetadataLog.should_log?(MapSet.new(["a", "b"]), refute_set, 1_000, 1_500, hb)
    end

    test "logs again once the heartbeat window elapses", %{hb: hb} do
      set = MapSet.new(["a"])
      assert MetadataLog.should_log?(set, set, 1_000, 1_000 + hb, hb)
    end
  end
end
