defmodule KafkaEx.Protocol.Kayrock.OffsetCommit.RequestTest do
  use ExUnit.Case, async: true

  alias KafkaEx.Protocol.Kayrock.OffsetCommit
  alias KafkaEx.Protocol.Kayrock.OffsetCommit.RequestHelpers

  describe "RequestHelpers.extract_common_fields/1" do
    test "extracts group_id from opts" do
      opts = [group_id: "test-group", other: "value"]

      result = RequestHelpers.extract_common_fields(opts)

      assert %{group_id: "test-group"} = result
    end

    test "raises when group_id is missing" do
      opts = [other: "value"]

      assert_raise KeyError, fn ->
        RequestHelpers.extract_common_fields(opts)
      end
    end
  end

  describe "RequestHelpers.extract_coordination_fields/1" do
    test "extracts generation_id and member_id with defaults" do
      opts = [group_id: "test-group"]

      result = RequestHelpers.extract_coordination_fields(opts)

      assert %{generation_id: -1, member_id: ""} = result
    end

    test "extracts custom generation_id and member_id" do
      opts = [group_id: "test-group", generation_id: 5, member_id: "member-123"]

      result = RequestHelpers.extract_coordination_fields(opts)

      assert %{generation_id: 5, member_id: "member-123"} = result
    end

    test "uses default when only generation_id is provided" do
      opts = [generation_id: 10]

      result = RequestHelpers.extract_coordination_fields(opts)

      assert %{generation_id: 10, member_id: ""} = result
    end

    test "uses default when only member_id is provided" do
      opts = [member_id: "consumer-1"]

      result = RequestHelpers.extract_coordination_fields(opts)

      assert %{generation_id: -1, member_id: "consumer-1"} = result
    end
  end

  describe "RequestHelpers.extract_retention_time/1" do
    test "returns default retention_time when not provided" do
      opts = [group_id: "test-group"]

      result = RequestHelpers.extract_retention_time(opts)

      assert %{retention_time: -1} = result
    end

    test "extracts custom retention_time" do
      opts = [retention_time: 86_400_000]

      result = RequestHelpers.extract_retention_time(opts)

      assert %{retention_time: 86_400_000} = result
    end
  end

  describe "RequestHelpers.build_partitions/2" do
    test "builds partitions without timestamp" do
      partitions = [
        %{partition_num: 0, offset: 100, metadata: "meta1"},
        %{partition_num: 1, offset: 200}
      ]

      result = RequestHelpers.build_partitions(partitions, false)

      assert [
               %{partition_index: 0, committed_offset: 100, committed_metadata: "meta1"},
               %{partition_index: 1, committed_offset: 200, committed_metadata: ""}
             ] = result
    end

    test "builds partitions with timestamp" do
      partitions = [
        %{partition_num: 0, offset: 100, timestamp: 1_234_567_890},
        %{partition_num: 1, offset: 200}
      ]

      result = RequestHelpers.build_partitions(partitions, true)

      assert [
               %{partition_index: 0, committed_offset: 100, committed_metadata: "", commit_timestamp: 1_234_567_890},
               %{partition_index: 1, committed_offset: 200, committed_metadata: "", commit_timestamp: -1}
             ] = result
    end

    test "uses default empty string for missing metadata" do
      partitions = [%{partition_num: 0, offset: 100}]

      result = RequestHelpers.build_partitions(partitions, false)

      assert [%{partition_index: 0, committed_offset: 100, committed_metadata: ""}] = result
    end

    test "uses default -1 for missing timestamp when include_timestamp is true" do
      partitions = [%{partition_num: 0, offset: 100}]

      result = RequestHelpers.build_partitions(partitions, true)

      assert [%{partition_index: 0, committed_offset: 100, committed_metadata: "", commit_timestamp: -1}] = result
    end
  end

  describe "RequestHelpers.build_topics/2" do
    test "builds topics structure without timestamp" do
      opts = [
        topics: [
          {"topic1", [%{partition_num: 0, offset: 100}]},
          {"topic2", [%{partition_num: 0, offset: 200}, %{partition_num: 1, offset: 300}]}
        ]
      ]

      result = RequestHelpers.build_topics(opts, false)

      assert [
               %{
                 name: "topic1",
                 partitions: [%{partition_index: 0, committed_offset: 100, committed_metadata: ""}]
               },
               %{
                 name: "topic2",
                 partitions: [
                   %{partition_index: 0, committed_offset: 200, committed_metadata: ""},
                   %{partition_index: 1, committed_offset: 300, committed_metadata: ""}
                 ]
               }
             ] = result
    end

    test "builds topics structure with timestamp" do
      opts = [
        topics: [
          {"topic1", [%{partition_num: 0, offset: 100, timestamp: 1_234_567_890}]}
        ]
      ]

      result = RequestHelpers.build_topics(opts, true)

      assert [
               %{
                 name: "topic1",
                 partitions: [
                   %{partition_index: 0, committed_offset: 100, committed_metadata: "", commit_timestamp: 1_234_567_890}
                 ]
               }
             ] = result
    end

    test "handles multiple partitions with mixed metadata" do
      opts = [
        topics: [
          {"topic1",
           [
             %{partition_num: 0, offset: 100, metadata: "custom"},
             %{partition_num: 1, offset: 200}
           ]}
        ]
      ]

      result = RequestHelpers.build_topics(opts, false)

      assert [
               %{
                 name: "topic1",
                 partitions: [
                   %{partition_index: 0, committed_offset: 100, committed_metadata: "custom"},
                   %{partition_index: 1, committed_offset: 200, committed_metadata: ""}
                 ]
               }
             ] = result
    end

    test "raises when topics key is missing" do
      opts = [group_id: "test-group"]

      assert_raise KeyError, fn ->
        RequestHelpers.build_topics(opts, false)
      end
    end
  end

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
end
