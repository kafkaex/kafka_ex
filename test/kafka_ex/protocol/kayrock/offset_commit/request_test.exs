defmodule KafkaEx.Protocol.Kayrock.OffsetCommit.RequestTest do
  use ExUnit.Case, async: true

  alias KafkaEx.Protocol.Kayrock.OffsetCommit

  describe "V0 Request implementation" do
    test "builds request with single topic and partition" do
      request = %Kayrock.OffsetCommit.V0.Request{}

      opts = [
        group_id: "test-group",
        topics: [
          {"test-topic", [%{partition_num: 0, offset: 100}]}
        ]
      ]

      result = OffsetCommit.Request.build_request(request, opts)

      assert result == %Kayrock.OffsetCommit.V0.Request{
               client_id: nil,
               correlation_id: nil,
               group_id: "test-group",
               topics: [
                 %{
                   name: "test-topic",
                   partitions: [
                     %{partition_index: 0, committed_offset: 100, committed_metadata: ""}
                   ]
                 }
               ]
             }
    end

    test "builds request with metadata" do
      request = %Kayrock.OffsetCommit.V0.Request{}

      opts = [
        group_id: "consumer-group",
        topics: [
          {"topic-1", [%{partition_num: 0, offset: 42, metadata: "consumer-instance-1"}]}
        ]
      ]

      result = OffsetCommit.Request.build_request(request, opts)

      assert result == %Kayrock.OffsetCommit.V0.Request{
               client_id: nil,
               correlation_id: nil,
               group_id: "consumer-group",
               topics: [
                 %{
                   name: "topic-1",
                   partitions: [
                     %{partition_index: 0, committed_offset: 42, committed_metadata: "consumer-instance-1"}
                   ]
                 }
               ]
             }
    end

    test "builds request with multiple partitions" do
      request = %Kayrock.OffsetCommit.V0.Request{}

      opts = [
        group_id: "multi-partition-group",
        topics: [
          {"topic-1",
           [
             %{partition_num: 0, offset: 10},
             %{partition_num: 1, offset: 20},
             %{partition_num: 2, offset: 30}
           ]}
        ]
      ]

      result = OffsetCommit.Request.build_request(request, opts)

      assert result == %Kayrock.OffsetCommit.V0.Request{
               client_id: nil,
               correlation_id: nil,
               group_id: "multi-partition-group",
               topics: [
                 %{
                   name: "topic-1",
                   partitions: [
                     %{partition_index: 0, committed_offset: 10, committed_metadata: ""},
                     %{partition_index: 1, committed_offset: 20, committed_metadata: ""},
                     %{partition_index: 2, committed_offset: 30, committed_metadata: ""}
                   ]
                 }
               ]
             }
    end

    test "builds request with multiple topics" do
      request = %Kayrock.OffsetCommit.V0.Request{}

      opts = [
        group_id: "multi-topic-group",
        topics: [
          {"topic-a", [%{partition_num: 0, offset: 100}]},
          {"topic-b", [%{partition_num: 0, offset: 200}, %{partition_num: 1, offset: 300}]}
        ]
      ]

      result = OffsetCommit.Request.build_request(request, opts)

      assert result == %Kayrock.OffsetCommit.V0.Request{
               client_id: nil,
               correlation_id: nil,
               group_id: "multi-topic-group",
               topics: [
                 %{
                   name: "topic-a",
                   partitions: [%{partition_index: 0, committed_offset: 100, committed_metadata: ""}]
                 },
                 %{
                   name: "topic-b",
                   partitions: [
                     %{partition_index: 0, committed_offset: 200, committed_metadata: ""},
                     %{partition_index: 1, committed_offset: 300, committed_metadata: ""}
                   ]
                 }
               ]
             }
    end

    test "builds request with empty topics list" do
      request = %Kayrock.OffsetCommit.V0.Request{}

      opts = [
        group_id: "empty-group",
        topics: []
      ]

      result = OffsetCommit.Request.build_request(request, opts)

      assert result == %Kayrock.OffsetCommit.V0.Request{
               client_id: nil,
               correlation_id: nil,
               group_id: "empty-group",
               topics: []
             }
    end
  end

  describe "V1 Request implementation" do
    test "builds request with generation_id and member_id" do
      request = %Kayrock.OffsetCommit.V1.Request{}

      opts = [
        group_id: "consumer-group",
        generation_id: 5,
        member_id: "consumer-1",
        topics: [
          {"test-topic", [%{partition_num: 0, offset: 100, timestamp: 1_234_567_890}]}
        ]
      ]

      result = OffsetCommit.Request.build_request(request, opts)

      assert result == %Kayrock.OffsetCommit.V1.Request{
               client_id: nil,
               correlation_id: nil,
               group_id: "consumer-group",
               generation_id: 5,
               member_id: "consumer-1",
               topics: [
                 %{
                   name: "test-topic",
                   partitions: [
                     %{
                       partition_index: 0,
                       committed_offset: 100,
                       commit_timestamp: 1_234_567_890,
                       committed_metadata: ""
                     }
                   ]
                 }
               ]
             }
    end

    test "builds request with default generation_id and member_id" do
      request = %Kayrock.OffsetCommit.V1.Request{}

      opts = [
        group_id: "simple-group",
        topics: [
          {"topic-1", [%{partition_num: 0, offset: 42}]}
        ]
      ]

      result = OffsetCommit.Request.build_request(request, opts)

      assert result == %Kayrock.OffsetCommit.V1.Request{
               client_id: nil,
               correlation_id: nil,
               group_id: "simple-group",
               generation_id: -1,
               member_id: "",
               topics: [
                 %{
                   name: "topic-1",
                   partitions: [
                     %{partition_index: 0, committed_offset: 42, commit_timestamp: -1, committed_metadata: ""}
                   ]
                 }
               ]
             }
    end

    test "builds request with timestamp and metadata" do
      request = %Kayrock.OffsetCommit.V1.Request{}

      opts = [
        group_id: "v1-group",
        generation_id: 10,
        member_id: "member-abc",
        topics: [
          {"topic-1", [%{partition_num: 0, offset: 500, timestamp: 9_999_999, metadata: "custom-meta"}]}
        ]
      ]

      result = OffsetCommit.Request.build_request(request, opts)

      assert result == %Kayrock.OffsetCommit.V1.Request{
               client_id: nil,
               correlation_id: nil,
               group_id: "v1-group",
               generation_id: 10,
               member_id: "member-abc",
               topics: [
                 %{
                   name: "topic-1",
                   partitions: [
                     %{
                       partition_index: 0,
                       committed_offset: 500,
                       commit_timestamp: 9_999_999,
                       committed_metadata: "custom-meta"
                     }
                   ]
                 }
               ]
             }
    end

    test "builds request with multiple partitions" do
      request = %Kayrock.OffsetCommit.V1.Request{}

      opts = [
        group_id: "multi-part-group",
        generation_id: 3,
        member_id: "consumer-x",
        topics: [
          {"topic-1",
           [
             %{partition_num: 0, offset: 10, timestamp: 100},
             %{partition_num: 1, offset: 20, timestamp: 200},
             %{partition_num: 2, offset: 30, timestamp: 300}
           ]}
        ]
      ]

      result = OffsetCommit.Request.build_request(request, opts)

      assert result.generation_id == 3
      assert result.member_id == "consumer-x"
      topic = hd(result.topics)
      assert length(topic.partitions) == 3

      assert Enum.at(topic.partitions, 0) == %{
               partition_index: 0,
               committed_offset: 10,
               commit_timestamp: 100,
               committed_metadata: ""
             }

      assert Enum.at(topic.partitions, 1) == %{
               partition_index: 1,
               committed_offset: 20,
               commit_timestamp: 200,
               committed_metadata: ""
             }

      assert Enum.at(topic.partitions, 2) == %{
               partition_index: 2,
               committed_offset: 30,
               commit_timestamp: 300,
               committed_metadata: ""
             }
    end

    test "builds request with multiple topics" do
      request = %Kayrock.OffsetCommit.V1.Request{}

      opts = [
        group_id: "multi-topic-v1",
        generation_id: 7,
        member_id: "member-123",
        topics: [
          {"topic-a", [%{partition_num: 0, offset: 100, timestamp: 1000}]},
          {"topic-b", [%{partition_num: 0, offset: 200, timestamp: 2000}]}
        ]
      ]

      result = OffsetCommit.Request.build_request(request, opts)

      assert length(result.topics) == 2
      assert Enum.at(result.topics, 0).name == "topic-a"
      assert Enum.at(result.topics, 1).name == "topic-b"
    end
  end

  describe "V2 Request implementation" do
    test "builds request with retention_time" do
      request = %Kayrock.OffsetCommit.V2.Request{}

      opts = [
        group_id: "v2-group",
        generation_id: 10,
        member_id: "member-v2",
        retention_time: 86_400_000,
        topics: [
          {"test-topic", [%{partition_num: 0, offset: 100}]}
        ]
      ]

      result = OffsetCommit.Request.build_request(request, opts)

      assert result == %Kayrock.OffsetCommit.V2.Request{
               client_id: nil,
               correlation_id: nil,
               group_id: "v2-group",
               generation_id: 10,
               member_id: "member-v2",
               retention_time_ms: 86_400_000,
               topics: [
                 %{
                   name: "test-topic",
                   partitions: [
                     %{partition_index: 0, committed_offset: 100, committed_metadata: ""}
                   ]
                 }
               ]
             }
    end

    test "builds request with default retention_time" do
      request = %Kayrock.OffsetCommit.V2.Request{}

      opts = [
        group_id: "default-retention-group",
        topics: [
          {"topic-1", [%{partition_num: 0, offset: 42}]}
        ]
      ]

      result = OffsetCommit.Request.build_request(request, opts)

      assert result == %Kayrock.OffsetCommit.V2.Request{
               client_id: nil,
               correlation_id: nil,
               group_id: "default-retention-group",
               generation_id: -1,
               member_id: "",
               retention_time_ms: -1,
               topics: [
                 %{
                   name: "topic-1",
                   partitions: [
                     %{partition_index: 0, committed_offset: 42, committed_metadata: ""}
                   ]
                 }
               ]
             }
    end

    test "builds request without timestamp field (managed by Kafka)" do
      request = %Kayrock.OffsetCommit.V2.Request{}

      opts = [
        group_id: "kafka-managed-group",
        generation_id: 5,
        member_id: "consumer-1",
        retention_time: 3_600_000,
        topics: [
          {"topic-1", [%{partition_num: 0, offset: 200, metadata: "meta-1"}]}
        ]
      ]

      result = OffsetCommit.Request.build_request(request, opts)

      partition = hd(hd(result.topics).partitions)
      assert partition.partition_index == 0
      assert partition.committed_offset == 200
      assert partition.committed_metadata == "meta-1"
      # Note: V2 does not have timestamp field
      refute Map.has_key?(partition, :commit_timestamp)
    end

    test "builds request with multiple partitions" do
      request = %Kayrock.OffsetCommit.V2.Request{}

      opts = [
        group_id: "multi-v2",
        generation_id: 8,
        member_id: "member-8",
        retention_time: 7_200_000,
        topics: [
          {"topic-1",
           [
             %{partition_num: 0, offset: 10},
             %{partition_num: 1, offset: 20},
             %{partition_num: 2, offset: 30}
           ]}
        ]
      ]

      result = OffsetCommit.Request.build_request(request, opts)

      assert result.retention_time_ms == 7_200_000
      topic = hd(result.topics)
      assert length(topic.partitions) == 3
    end

    test "builds request with multiple topics" do
      request = %Kayrock.OffsetCommit.V2.Request{}

      opts = [
        group_id: "multi-topic-v2",
        generation_id: 12,
        member_id: "member-12",
        retention_time: 86_400_000,
        topics: [
          {"topic-a", [%{partition_num: 0, offset: 100}]},
          {"topic-b", [%{partition_num: 0, offset: 200}, %{partition_num: 1, offset: 300}]}
        ]
      ]

      result = OffsetCommit.Request.build_request(request, opts)

      assert length(result.topics) == 2
      assert Enum.at(result.topics, 0).name == "topic-a"
      assert Enum.at(result.topics, 1).name == "topic-b"
    end
  end

  describe "V3 Request implementation" do
    test "builds request with retention_time (same structure as V2)" do
      request = %Kayrock.OffsetCommit.V3.Request{}

      opts = [
        group_id: "v3-group",
        generation_id: 15,
        member_id: "member-v3",
        retention_time: 86_400_000,
        topics: [
          {"test-topic", [%{partition_num: 0, offset: 500}]}
        ]
      ]

      result = OffsetCommit.Request.build_request(request, opts)

      assert result == %Kayrock.OffsetCommit.V3.Request{
               client_id: nil,
               correlation_id: nil,
               group_id: "v3-group",
               generation_id: 15,
               member_id: "member-v3",
               retention_time_ms: 86_400_000,
               topics: [
                 %{
                   name: "test-topic",
                   partitions: [
                     %{partition_index: 0, committed_offset: 500, committed_metadata: ""}
                   ]
                 }
               ]
             }
    end

    test "builds request with default retention_time" do
      request = %Kayrock.OffsetCommit.V3.Request{}

      opts = [
        group_id: "default-v3-group",
        topics: [
          {"topic-1", [%{partition_num: 0, offset: 42}]}
        ]
      ]

      result = OffsetCommit.Request.build_request(request, opts)

      assert result.group_id == "default-v3-group"
      assert result.generation_id == -1
      assert result.member_id == ""
      assert result.retention_time_ms == -1
      assert length(result.topics) == 1
    end

    test "builds request with multiple topics and partitions" do
      request = %Kayrock.OffsetCommit.V3.Request{}

      opts = [
        group_id: "multi-v3",
        generation_id: 20,
        member_id: "member-20",
        retention_time: 7_200_000,
        topics: [
          {"topic-a", [%{partition_num: 0, offset: 100}, %{partition_num: 1, offset: 200}]},
          {"topic-b", [%{partition_num: 0, offset: 300, metadata: "committed"}]}
        ]
      ]

      result = OffsetCommit.Request.build_request(request, opts)

      assert result.retention_time_ms == 7_200_000
      assert length(result.topics) == 2

      [topic_a, topic_b] = result.topics
      assert topic_a.name == "topic-a"
      assert length(topic_a.partitions) == 2

      assert topic_b.name == "topic-b"
      [partition] = topic_b.partitions
      assert partition.committed_metadata == "committed"
    end

    test "V3 does not include timestamp field in partitions" do
      request = %Kayrock.OffsetCommit.V3.Request{}

      opts = [
        group_id: "no-timestamp-v3",
        generation_id: 1,
        member_id: "m-1",
        topics: [
          {"topic-1", [%{partition_num: 0, offset: 100, timestamp: 9999}]}
        ]
      ]

      result = OffsetCommit.Request.build_request(request, opts)

      partition = hd(hd(result.topics).partitions)
      refute Map.has_key?(partition, :commit_timestamp)
    end

    test "can serialize the built request" do
      request = %Kayrock.OffsetCommit.V3.Request{}

      opts = [
        group_id: "serialize-v3",
        generation_id: 1,
        member_id: "m-1",
        topics: [
          {"topic-1", [%{partition_num: 0, offset: 100}]}
        ]
      ]

      result = OffsetCommit.Request.build_request(request, opts)

      result_with_client_data = %{result | client_id: "test-client", correlation_id: 1}
      assert Kayrock.OffsetCommit.V3.Request.serialize(result_with_client_data)
    end
  end

  describe "V4 Request implementation" do
    test "builds request identical to V3 (pure version bump)" do
      request = %Kayrock.OffsetCommit.V4.Request{}

      opts = [
        group_id: "v4-group",
        generation_id: 15,
        member_id: "member-v4",
        retention_time: 86_400_000,
        topics: [
          {"test-topic", [%{partition_num: 0, offset: 500}]}
        ]
      ]

      result = OffsetCommit.Request.build_request(request, opts)

      assert result == %Kayrock.OffsetCommit.V4.Request{
               client_id: nil,
               correlation_id: nil,
               group_id: "v4-group",
               generation_id: 15,
               member_id: "member-v4",
               retention_time_ms: 86_400_000,
               topics: [
                 %{
                   name: "test-topic",
                   partitions: [
                     %{partition_index: 0, committed_offset: 500, committed_metadata: ""}
                   ]
                 }
               ]
             }
    end

    test "builds request with default retention_time" do
      request = %Kayrock.OffsetCommit.V4.Request{}

      opts = [
        group_id: "default-v4-group",
        topics: [
          {"topic-1", [%{partition_num: 0, offset: 42}]}
        ]
      ]

      result = OffsetCommit.Request.build_request(request, opts)

      assert result.group_id == "default-v4-group"
      assert result.generation_id == -1
      assert result.member_id == ""
      assert result.retention_time_ms == -1
      assert length(result.topics) == 1
    end

    test "builds request with multiple topics and partitions" do
      request = %Kayrock.OffsetCommit.V4.Request{}

      opts = [
        group_id: "multi-v4",
        generation_id: 20,
        member_id: "member-20",
        retention_time: 7_200_000,
        topics: [
          {"topic-a", [%{partition_num: 0, offset: 100}, %{partition_num: 1, offset: 200}]},
          {"topic-b", [%{partition_num: 0, offset: 300, metadata: "committed"}]}
        ]
      ]

      result = OffsetCommit.Request.build_request(request, opts)

      assert result.retention_time_ms == 7_200_000
      assert length(result.topics) == 2

      [topic_a, topic_b] = result.topics
      assert topic_a.name == "topic-a"
      assert length(topic_a.partitions) == 2

      assert topic_b.name == "topic-b"
      [partition] = topic_b.partitions
      assert partition.committed_metadata == "committed"
    end

    test "can serialize the built request" do
      request = %Kayrock.OffsetCommit.V4.Request{}

      opts = [
        group_id: "serialize-v4",
        generation_id: 1,
        member_id: "m-1",
        topics: [
          {"topic-1", [%{partition_num: 0, offset: 100}]}
        ]
      ]

      result = OffsetCommit.Request.build_request(request, opts)

      result_with_client_data = %{result | client_id: "test-client", correlation_id: 1}
      assert Kayrock.OffsetCommit.V4.Request.serialize(result_with_client_data)
    end
  end

  describe "V5 Request implementation" do
    test "builds request without retention_time_ms" do
      request = %Kayrock.OffsetCommit.V5.Request{}

      opts = [
        group_id: "v5-group",
        generation_id: 10,
        member_id: "member-v5",
        topics: [
          {"test-topic", [%{partition_num: 0, offset: 100}]}
        ]
      ]

      result = OffsetCommit.Request.build_request(request, opts)

      assert result == %Kayrock.OffsetCommit.V5.Request{
               client_id: nil,
               correlation_id: nil,
               group_id: "v5-group",
               generation_id: 10,
               member_id: "member-v5",
               topics: [
                 %{
                   name: "test-topic",
                   partitions: [
                     %{partition_index: 0, committed_offset: 100, committed_metadata: ""}
                   ]
                 }
               ]
             }
    end

    test "V5 struct does not have retention_time_ms field" do
      request = %Kayrock.OffsetCommit.V5.Request{}

      refute Map.has_key?(request, :retention_time_ms)
    end

    test "builds request with default generation_id and member_id" do
      request = %Kayrock.OffsetCommit.V5.Request{}

      opts = [
        group_id: "default-v5-group",
        topics: [
          {"topic-1", [%{partition_num: 0, offset: 42}]}
        ]
      ]

      result = OffsetCommit.Request.build_request(request, opts)

      assert result.group_id == "default-v5-group"
      assert result.generation_id == -1
      assert result.member_id == ""
    end

    test "builds request with multiple topics" do
      request = %Kayrock.OffsetCommit.V5.Request{}

      opts = [
        group_id: "multi-v5",
        generation_id: 8,
        member_id: "member-8",
        topics: [
          {"topic-a", [%{partition_num: 0, offset: 100}]},
          {"topic-b", [%{partition_num: 0, offset: 200}, %{partition_num: 1, offset: 300}]}
        ]
      ]

      result = OffsetCommit.Request.build_request(request, opts)

      assert length(result.topics) == 2
      assert Enum.at(result.topics, 0).name == "topic-a"
      assert Enum.at(result.topics, 1).name == "topic-b"
    end

    test "can serialize the built request" do
      request = %Kayrock.OffsetCommit.V5.Request{}

      opts = [
        group_id: "serialize-v5",
        generation_id: 1,
        member_id: "m-1",
        topics: [
          {"topic-1", [%{partition_num: 0, offset: 100}]}
        ]
      ]

      result = OffsetCommit.Request.build_request(request, opts)

      result_with_client_data = %{result | client_id: "test-client", correlation_id: 1}
      assert Kayrock.OffsetCommit.V5.Request.serialize(result_with_client_data)
    end
  end

  describe "V6 Request implementation" do
    test "builds request with committed_leader_epoch per partition" do
      request = %Kayrock.OffsetCommit.V6.Request{}

      opts = [
        group_id: "v6-group",
        generation_id: 12,
        member_id: "member-v6",
        topics: [
          {"test-topic", [%{partition_num: 0, offset: 100, leader_epoch: 5}]}
        ]
      ]

      result = OffsetCommit.Request.build_request(request, opts)

      assert result == %Kayrock.OffsetCommit.V6.Request{
               client_id: nil,
               correlation_id: nil,
               group_id: "v6-group",
               generation_id: 12,
               member_id: "member-v6",
               topics: [
                 %{
                   name: "test-topic",
                   partitions: [
                     %{
                       partition_index: 0,
                       committed_offset: 100,
                       committed_metadata: "",
                       committed_leader_epoch: 5
                     }
                   ]
                 }
               ]
             }
    end

    test "defaults committed_leader_epoch to -1 when not provided" do
      request = %Kayrock.OffsetCommit.V6.Request{}

      opts = [
        group_id: "v6-default-epoch",
        topics: [
          {"topic-1", [%{partition_num: 0, offset: 42}]}
        ]
      ]

      result = OffsetCommit.Request.build_request(request, opts)

      partition = hd(hd(result.topics).partitions)
      assert partition.committed_leader_epoch == -1
    end

    test "V6 does not have retention_time_ms" do
      request = %Kayrock.OffsetCommit.V6.Request{}

      refute Map.has_key?(request, :retention_time_ms)
    end

    test "V6 does not have group_instance_id" do
      request = %Kayrock.OffsetCommit.V6.Request{}

      refute Map.has_key?(request, :group_instance_id)
    end

    test "builds request with multiple partitions having different leader epochs" do
      request = %Kayrock.OffsetCommit.V6.Request{}

      opts = [
        group_id: "v6-multi-epoch",
        generation_id: 5,
        member_id: "member-5",
        topics: [
          {"topic-1",
           [
             %{partition_num: 0, offset: 10, leader_epoch: 3},
             %{partition_num: 1, offset: 20, leader_epoch: 5},
             %{partition_num: 2, offset: 30}
           ]}
        ]
      ]

      result = OffsetCommit.Request.build_request(request, opts)

      topic = hd(result.topics)
      assert length(topic.partitions) == 3

      assert Enum.at(topic.partitions, 0).committed_leader_epoch == 3
      assert Enum.at(topic.partitions, 1).committed_leader_epoch == 5
      assert Enum.at(topic.partitions, 2).committed_leader_epoch == -1
    end

    test "can serialize the built request" do
      request = %Kayrock.OffsetCommit.V6.Request{}

      opts = [
        group_id: "serialize-v6",
        generation_id: 1,
        member_id: "m-1",
        topics: [
          {"topic-1", [%{partition_num: 0, offset: 100, leader_epoch: 2}]}
        ]
      ]

      result = OffsetCommit.Request.build_request(request, opts)

      result_with_client_data = %{result | client_id: "test-client", correlation_id: 1}
      assert Kayrock.OffsetCommit.V6.Request.serialize(result_with_client_data)
    end
  end

  describe "V7 Request implementation" do
    test "builds request with group_instance_id" do
      request = %Kayrock.OffsetCommit.V7.Request{}

      opts = [
        group_id: "v7-group",
        generation_id: 15,
        member_id: "member-v7",
        group_instance_id: "static-member-1",
        topics: [
          {"test-topic", [%{partition_num: 0, offset: 200, leader_epoch: 7}]}
        ]
      ]

      result = OffsetCommit.Request.build_request(request, opts)

      assert result == %Kayrock.OffsetCommit.V7.Request{
               client_id: nil,
               correlation_id: nil,
               group_id: "v7-group",
               generation_id: 15,
               member_id: "member-v7",
               group_instance_id: "static-member-1",
               topics: [
                 %{
                   name: "test-topic",
                   partitions: [
                     %{
                       partition_index: 0,
                       committed_offset: 200,
                       committed_metadata: "",
                       committed_leader_epoch: 7
                     }
                   ]
                 }
               ]
             }
    end

    test "defaults group_instance_id to nil when not provided" do
      request = %Kayrock.OffsetCommit.V7.Request{}

      opts = [
        group_id: "v7-no-instance",
        topics: [
          {"topic-1", [%{partition_num: 0, offset: 42}]}
        ]
      ]

      result = OffsetCommit.Request.build_request(request, opts)

      assert result.group_instance_id == nil
      assert result.generation_id == -1
      assert result.member_id == ""
    end

    test "includes committed_leader_epoch from V6" do
      request = %Kayrock.OffsetCommit.V7.Request{}

      opts = [
        group_id: "v7-with-epoch",
        generation_id: 3,
        member_id: "m-3",
        group_instance_id: "instance-1",
        topics: [
          {"topic-1", [%{partition_num: 0, offset: 100, leader_epoch: 10}]}
        ]
      ]

      result = OffsetCommit.Request.build_request(request, opts)

      partition = hd(hd(result.topics).partitions)
      assert partition.committed_leader_epoch == 10
    end

    test "builds request with multiple topics and metadata" do
      request = %Kayrock.OffsetCommit.V7.Request{}

      opts = [
        group_id: "multi-v7",
        generation_id: 20,
        member_id: "member-20",
        group_instance_id: "static-20",
        topics: [
          {"topic-a", [%{partition_num: 0, offset: 100, metadata: "meta-a", leader_epoch: 1}]},
          {"topic-b", [%{partition_num: 0, offset: 200, leader_epoch: 2}, %{partition_num: 1, offset: 300}]}
        ]
      ]

      result = OffsetCommit.Request.build_request(request, opts)

      assert length(result.topics) == 2
      assert result.group_instance_id == "static-20"

      [topic_a, topic_b] = result.topics
      assert hd(topic_a.partitions).committed_metadata == "meta-a"
      assert length(topic_b.partitions) == 2
    end

    test "can serialize the built request" do
      request = %Kayrock.OffsetCommit.V7.Request{}

      opts = [
        group_id: "serialize-v7",
        generation_id: 1,
        member_id: "m-1",
        group_instance_id: "instance-1",
        topics: [
          {"topic-1", [%{partition_num: 0, offset: 100, leader_epoch: 2}]}
        ]
      ]

      result = OffsetCommit.Request.build_request(request, opts)

      result_with_client_data = %{result | client_id: "test-client", correlation_id: 1}
      assert Kayrock.OffsetCommit.V7.Request.serialize(result_with_client_data)
    end
  end

  describe "V8 Request implementation (flexible version)" do
    test "builds request identical to V7 (Kayrock handles compact encoding)" do
      request = %Kayrock.OffsetCommit.V8.Request{}

      opts = [
        group_id: "v8-group",
        generation_id: 25,
        member_id: "member-v8",
        group_instance_id: "static-v8",
        topics: [
          {"test-topic", [%{partition_num: 0, offset: 1000, leader_epoch: 12}]}
        ]
      ]

      result = OffsetCommit.Request.build_request(request, opts)

      assert result.group_id == "v8-group"
      assert result.generation_id == 25
      assert result.member_id == "member-v8"
      assert result.group_instance_id == "static-v8"

      [topic] = result.topics
      [partition] = topic.partitions
      assert partition.committed_leader_epoch == 12
    end

    test "V8 struct has tagged_fields field" do
      request = %Kayrock.OffsetCommit.V8.Request{}

      assert Map.has_key?(request, :tagged_fields)
      assert request.tagged_fields == []
    end

    test "defaults group_instance_id to nil" do
      request = %Kayrock.OffsetCommit.V8.Request{}

      opts = [
        group_id: "v8-no-instance",
        topics: [
          {"topic-1", [%{partition_num: 0, offset: 42}]}
        ]
      ]

      result = OffsetCommit.Request.build_request(request, opts)

      assert result.group_instance_id == nil
    end

    test "builds request with multiple topics and partitions" do
      request = %Kayrock.OffsetCommit.V8.Request{}

      opts = [
        group_id: "multi-v8",
        generation_id: 30,
        member_id: "member-30",
        group_instance_id: "static-30",
        topics: [
          {"topic-a", [%{partition_num: 0, offset: 100, leader_epoch: 1, metadata: "a-meta"}]},
          {"topic-b",
           [
             %{partition_num: 0, offset: 200, leader_epoch: 2},
             %{partition_num: 1, offset: 300, leader_epoch: 3}
           ]}
        ]
      ]

      result = OffsetCommit.Request.build_request(request, opts)

      assert length(result.topics) == 2
      [topic_a, topic_b] = result.topics
      assert hd(topic_a.partitions).committed_metadata == "a-meta"
      assert length(topic_b.partitions) == 2
      assert Enum.at(topic_b.partitions, 0).committed_leader_epoch == 2
      assert Enum.at(topic_b.partitions, 1).committed_leader_epoch == 3
    end

    test "can serialize the built request" do
      request = %Kayrock.OffsetCommit.V8.Request{}

      opts = [
        group_id: "serialize-v8",
        generation_id: 1,
        member_id: "m-1",
        group_instance_id: "instance-1",
        topics: [
          {"topic-1", [%{partition_num: 0, offset: 100, leader_epoch: 2}]}
        ]
      ]

      result = OffsetCommit.Request.build_request(request, opts)

      result_with_client_data = %{result | client_id: "test-client", correlation_id: 1}
      assert Kayrock.OffsetCommit.V8.Request.serialize(result_with_client_data)
    end
  end

  describe "Cross-version request consistency" do
    @base_opts [
      group_id: "cross-version-group",
      generation_id: 5,
      member_id: "member-5",
      topics: [
        {"test-topic", [%{partition_num: 0, offset: 100}]}
      ]
    ]

    test "V2, V3, and V4 requests are structurally identical" do
      v2 =
        OffsetCommit.Request.build_request(%Kayrock.OffsetCommit.V2.Request{}, @base_opts ++ [retention_time: 60_000])

      v3 =
        OffsetCommit.Request.build_request(%Kayrock.OffsetCommit.V3.Request{}, @base_opts ++ [retention_time: 60_000])

      v4 =
        OffsetCommit.Request.build_request(%Kayrock.OffsetCommit.V4.Request{}, @base_opts ++ [retention_time: 60_000])

      # All should have same data fields (minus the struct type)
      assert v2.group_id == v3.group_id
      assert v3.group_id == v4.group_id
      assert v2.retention_time_ms == v3.retention_time_ms
      assert v3.retention_time_ms == v4.retention_time_ms
      assert v2.topics == v3.topics
      assert v3.topics == v4.topics
    end

    test "V5 removes retention_time_ms compared to V4" do
      v4 = OffsetCommit.Request.build_request(%Kayrock.OffsetCommit.V4.Request{}, @base_opts)
      v5 = OffsetCommit.Request.build_request(%Kayrock.OffsetCommit.V5.Request{}, @base_opts)

      assert Map.has_key?(v4, :retention_time_ms)
      refute Map.has_key?(v5, :retention_time_ms)
      assert v4.group_id == v5.group_id
      assert v4.generation_id == v5.generation_id
    end

    test "V6 adds committed_leader_epoch compared to V5" do
      v5 = OffsetCommit.Request.build_request(%Kayrock.OffsetCommit.V5.Request{}, @base_opts)
      v6 = OffsetCommit.Request.build_request(%Kayrock.OffsetCommit.V6.Request{}, @base_opts)

      v5_partition = hd(hd(v5.topics).partitions)
      v6_partition = hd(hd(v6.topics).partitions)

      refute Map.has_key?(v5_partition, :committed_leader_epoch)
      assert Map.has_key?(v6_partition, :committed_leader_epoch)
      assert v6_partition.committed_leader_epoch == -1
    end

    test "V7 adds group_instance_id compared to V6" do
      v6 = OffsetCommit.Request.build_request(%Kayrock.OffsetCommit.V6.Request{}, @base_opts)
      v7 = OffsetCommit.Request.build_request(%Kayrock.OffsetCommit.V7.Request{}, @base_opts)

      refute Map.has_key?(v6, :group_instance_id)
      assert Map.has_key?(v7, :group_instance_id)
      assert v7.group_instance_id == nil
    end

    test "V8 has tagged_fields compared to V7" do
      v7 = OffsetCommit.Request.build_request(%Kayrock.OffsetCommit.V7.Request{}, @base_opts)
      v8 = OffsetCommit.Request.build_request(%Kayrock.OffsetCommit.V8.Request{}, @base_opts)

      refute Map.has_key?(v7, :tagged_fields)
      assert Map.has_key?(v8, :tagged_fields)
      assert v8.group_id == v7.group_id
      assert v8.group_instance_id == v7.group_instance_id
    end
  end

  describe "Any Request fallback implementation" do
    test "routes V7+-like map (has group_instance_id) to build_v7_plus_request" do
      # Plain map with group_instance_id key triggers V7+ path
      request_template = %{group_instance_id: nil, generation_id: nil, member_id: nil}

      opts = [
        group_id: "any-v7-group",
        generation_id: 5,
        member_id: "member-5",
        group_instance_id: "static-1",
        topics: [{"topic-1", [%{partition_num: 0, offset: 100, leader_epoch: 3}]}]
      ]

      result = OffsetCommit.Request.build_request(request_template, opts)

      assert result.group_id == "any-v7-group"
      assert result.group_instance_id == "static-1"

      partition = hd(hd(result.topics).partitions)
      assert partition.committed_leader_epoch == 3
    end

    test "routes V2/V3/V4-like map (has retention_time_ms) to build_v2_v3_request" do
      request_template = %{retention_time_ms: nil, generation_id: nil, member_id: nil}

      opts = [
        group_id: "any-v2-group",
        retention_time: 60_000,
        topics: [{"topic-1", [%{partition_num: 0, offset: 100}]}]
      ]

      result = OffsetCommit.Request.build_request(request_template, opts)

      assert result.group_id == "any-v2-group"
      assert result.retention_time_ms == 60_000
    end

    test "routes V5/V6-like map (has generation_id, no retention/instance) to build_v6_request" do
      # V5/V6 path: has generation_id but no retention_time_ms and no group_instance_id
      request_template = %{generation_id: nil, member_id: nil}

      opts = [
        group_id: "any-v5-group",
        generation_id: 10,
        member_id: "member-10",
        topics: [{"topic-1", [%{partition_num: 0, offset: 100, leader_epoch: 5}]}]
      ]

      result = OffsetCommit.Request.build_request(request_template, opts)

      assert result.group_id == "any-v5-group"
      assert result.generation_id == 10
      refute Map.has_key?(result, :retention_time_ms)
      refute Map.has_key?(result, :group_instance_id)

      # V6 path includes committed_leader_epoch
      partition = hd(hd(result.topics).partitions)
      assert partition.committed_leader_epoch == 5
    end

    test "routes V0-like map (no generation_id) to V0 fallback" do
      request_template = %{}

      opts = [
        group_id: "any-v0-group",
        topics: [{"topic-1", [%{partition_num: 0, offset: 42}]}]
      ]

      result = OffsetCommit.Request.build_request(request_template, opts)

      assert result.group_id == "any-v0-group"
      refute Map.has_key?(result, :generation_id)
      refute Map.has_key?(result, :retention_time_ms)
    end
  end
end
