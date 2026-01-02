defmodule KafkaEx.Protocol.KayrockProtocolTest do
  use ExUnit.Case, async: true

  alias KafkaEx.Protocol.KayrockProtocol

  describe "build_request/3" do
    test "builds api_versions request" do
      request = KayrockProtocol.build_request(:api_versions, 0, [])

      assert request != nil
    end

    test "builds metadata request" do
      request = KayrockProtocol.build_request(:metadata, 0, topics: nil)

      assert request != nil
    end

    test "builds list_offsets request" do
      request =
        KayrockProtocol.build_request(:list_offsets, 0,
          topics: [{"test-topic", [%{partition_num: 0, timestamp: -1}]}],
          replica_id: -1
        )

      assert request != nil
    end

    test "builds offset_fetch request" do
      request =
        KayrockProtocol.build_request(:offset_fetch, 0,
          group_id: "test-group",
          topics: [{"test-topic", [%{partition_num: 0}]}]
        )

      assert request != nil
    end

    test "builds offset_commit request" do
      request =
        KayrockProtocol.build_request(:offset_commit, 0,
          group_id: "test-group",
          topics: [{"test-topic", [%{partition_num: 0, offset: 100}]}]
        )

      assert request != nil
    end

    test "builds heartbeat request" do
      request =
        KayrockProtocol.build_request(:heartbeat, 0,
          group_id: "test-group",
          generation_id: 1,
          member_id: "member-1"
        )

      assert request != nil
    end

    test "builds join_group request" do
      request =
        KayrockProtocol.build_request(:join_group, 0,
          group_id: "test-group",
          session_timeout: 30_000,
          member_id: "",
          protocol_type: "consumer",
          group_protocols: []
        )

      assert request != nil
    end

    test "builds leave_group request" do
      request =
        KayrockProtocol.build_request(:leave_group, 0,
          group_id: "test-group",
          member_id: "member-1"
        )

      assert request != nil
    end

    test "builds sync_group request" do
      request =
        KayrockProtocol.build_request(:sync_group, 0,
          group_id: "test-group",
          generation_id: 1,
          member_id: "member-1",
          group_assignment: []
        )

      assert request != nil
    end

    test "builds produce request" do
      request =
        KayrockProtocol.build_request(:produce, 0,
          topic: "test-topic",
          partition: 0,
          messages: [%{value: "hello"}]
        )

      assert request != nil
    end

    test "builds fetch request" do
      request =
        KayrockProtocol.build_request(:fetch, 0,
          topic: "test-topic",
          partition: 0,
          offset: 0,
          max_bytes: 1_000_000,
          replica_id: -1
        )

      assert request != nil
    end

    test "builds find_coordinator request" do
      request =
        KayrockProtocol.build_request(:find_coordinator, 0, group_id: "test-group")

      assert request != nil
    end

    test "builds create_topics request" do
      request =
        KayrockProtocol.build_request(:create_topics, 0,
          topics: [%{topic: "new-topic", num_partitions: 1, replication_factor: 1}],
          timeout: 30_000
        )

      assert request != nil
    end

    test "builds delete_topics request" do
      request =
        KayrockProtocol.build_request(:delete_topics, 0,
          topics: ["topic-to-delete"],
          timeout: 30_000
        )

      assert request != nil
    end

    test "builds describe_groups request" do
      request =
        KayrockProtocol.build_request(:describe_groups, 0, group_names: ["test-group"])

      assert request != nil
    end
  end
end
