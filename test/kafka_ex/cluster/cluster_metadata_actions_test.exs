defmodule KafkaEx.Cluster.ClusterMetadataActionsTest do
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog

  alias KafkaEx.Client.NodeSelector
  alias KafkaEx.Cluster.ClusterMetadata
  alias KafkaEx.Support.Retry

  # Mirrors the atoms in Retry's guards, plus :unknown from the client's error extraction.
  @transport_errors [
    :timeout,
    :closed,
    :no_broker,
    :not_connected,
    :econnreset,
    :transport_error,
    :parse_error,
    :unknown
  ]
  @leadership_errors [
    :not_leader_for_partition,
    :leader_not_available,
    :not_leader_or_follower,
    :fenced_leader_epoch,
    :unknown_topic_or_partition,
    :unknown_topic_id
  ]
  @opts [rediscover_coordinator?: false, allow_auto_topic_creation: true]

  defp all_errors do
    {broker_errors, _log} =
      with_log(fn ->
        -1
        |> Stream.iterate(&(&1 + 1))
        |> Stream.map(&Kayrock.ErrorCode.code_to_atom/1)
        |> Enum.take_while(&(&1 != :unknown))
      end)

    Enum.uniq(broker_errors ++ @transport_errors)
  end

  defp refresh(topics, allow \\ true),
    do: {:refresh_metadata, %{topics: topics, allow_auto_topic_creation: allow, api_version: nil}}

  defp group, do: NodeSelector.consumer_group("g")

  defp other_selectors do
    [
      NodeSelector.topic_partition("t", 0),
      NodeSelector.topic_partition_replica("t", 0),
      NodeSelector.node_id(1),
      NodeSelector.controller(),
      NodeSelector.random(),
      NodeSelector.first_available()
    ]
  end

  describe "selection_miss_actions/2" do
    test "a partition lookup refreshes the metadata for its topic" do
      assert ClusterMetadata.selection_miss_actions(NodeSelector.topic_partition("t", 0), @opts) == [refresh(["t"])]

      assert ClusterMetadata.selection_miss_actions(NodeSelector.topic_partition_replica("t", 0), @opts) == [
               refresh(["t"])
             ]
    end

    test "the refresh carries the client's allow_auto_topic_creation" do
      opts = Keyword.put(@opts, :allow_auto_topic_creation, false)

      assert ClusterMetadata.selection_miss_actions(NodeSelector.topic_partition("t", 0), opts) == [
               refresh(["t"], false)
             ]
    end

    test "a coordinator lookup re-discovers the coordinator without dropping the cached one" do
      assert ClusterMetadata.selection_miss_actions(group(), @opts) == [{:rediscover_coordinator, "g"}]
    end

    test "any other lookup does nothing" do
      for selector <- [
            NodeSelector.node_id(1),
            NodeSelector.controller(),
            NodeSelector.random(),
            NodeSelector.first_available()
          ] do
        assert ClusterMetadata.selection_miss_actions(selector, @opts) == []
      end
    end
  end

  describe "error_actions/3" do
    test "the error universe holds every Kafka error decided on" do
      errors = all_errors()

      for error <- [:no_error, :not_coordinator, :coordinator_not_available | @leadership_errors],
          Kayrock.ErrorCode.atom_to_code(error) do
        assert error in errors
      end
    end

    test "no error is both a leadership and a coordinator-refresh error, so the branch order is irrelevant" do
      assert Enum.filter(all_errors(), &(Retry.leadership_error?(&1) and Retry.coordinator_refresh_error?(&1))) == []
    end

    test "a leadership error refreshes the tracked topics only, for any request" do
      for error <- @leadership_errors, selector <- [group() | other_selectors()] do
        assert ClusterMetadata.error_actions(error, selector, @opts) == [refresh([])]
      end
    end

    test "a coordinator error on a coordinator request drops the coordinator, then re-discovers it" do
      for error <- [:not_coordinator, :coordinator_not_available] do
        assert ClusterMetadata.error_actions(error, group(), @opts) ==
                 [{:drop_coordinator, "g"}, {:rediscover_coordinator, "g"}]
      end
    end

    test "a coordinator error on any other request does nothing" do
      for error <- [:not_coordinator, :coordinator_not_available], selector <- other_selectors() do
        assert ClusterMetadata.error_actions(error, selector, @opts) == []
      end
    end

    test "a connection error drops the coordinator only when the request asks for re-discovery" do
      rediscover = Keyword.put(@opts, :rediscover_coordinator?, true)

      for error <- [:timeout, :closed, :no_broker, :not_connected, :econnreset, :transport_error] do
        assert ClusterMetadata.error_actions(error, group(), rediscover) == [{:drop_coordinator, "g"}]
        assert ClusterMetadata.error_actions(error, group(), @opts) == []
      end
    end

    test "every error yields one of four action lists, so refresh and drop never combine" do
      rediscover = Keyword.put(@opts, :rediscover_coordinator?, true)

      allowed = [
        [],
        [refresh([])],
        [{:drop_coordinator, "g"}, {:rediscover_coordinator, "g"}],
        [{:drop_coordinator, "g"}]
      ]

      for error <- all_errors(), selector <- [group() | other_selectors()], opts <- [@opts, rediscover] do
        assert ClusterMetadata.error_actions(error, selector, opts) in allowed
      end
    end

    test "a send-once error never refreshes or re-discovers" do
      rediscover = Keyword.put(@opts, :rediscover_coordinator?, true)

      for error <- all_errors(),
          Retry.transport_timeout?(error) or Retry.consumer_group_heartbeat_send_once?(error) do
        assert ClusterMetadata.error_actions(error, group(), rediscover) in [[], [{:drop_coordinator, "g"}]]
      end
    end

    test ":unknown_topic_id only refreshes, even on a coordinator request that re-discovers" do
      rediscover = Keyword.put(@opts, :rediscover_coordinator?, true)

      assert ClusterMetadata.error_actions(:unknown_topic_id, group(), rediscover) == [refresh([])]
    end
  end
end
