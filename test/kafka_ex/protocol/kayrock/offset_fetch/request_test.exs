defmodule KafkaEx.Protocol.Kayrock.OffsetFetch.RequestTest do
  use ExUnit.Case, async: true

  alias KafkaEx.Protocol.Kayrock.OffsetFetch
  alias KafkaEx.Protocol.Kayrock.OffsetFetch.RequestHelpers

  describe "RequestHelpers.extract_common_fields/1" do
    test "extracts group_id from opts" do
      opts = [group_id: "consumer-group", other: "value"]

      result = RequestHelpers.extract_common_fields(opts)

      assert %{group_id: "consumer-group"} = result
    end

    test "raises when group_id is missing" do
      opts = [other: "value"]

      assert_raise KeyError, fn ->
        RequestHelpers.extract_common_fields(opts)
      end
    end
  end

  describe "RequestHelpers.build_partition_indexes/1" do
    test "builds partition index list with partition numbers" do
      partitions = [
        %{partition_num: 0},
        %{partition_num: 1},
        %{partition_num: 5}
      ]

      result = RequestHelpers.build_partition_indexes(partitions)

      assert [0, 1, 5] = result
    end

    test "handles single partition" do
      partitions = [%{partition_num: 0}]

      result = RequestHelpers.build_partition_indexes(partitions)

      assert [0] = result
    end

    test "ignores extra fields in partition data" do
      partitions = [
        %{partition_num: 0, offset: 100, metadata: "ignored"}
      ]

      result = RequestHelpers.build_partition_indexes(partitions)

      assert [0] = result
    end
  end

  describe "RequestHelpers.build_topics/1" do
    test "builds topics structure with partitions" do
      opts = [
        topics: [
          {"topic1", [%{partition_num: 0}]},
          {"topic2", [%{partition_num: 0}, %{partition_num: 1}]}
        ]
      ]

      result = RequestHelpers.build_topics(opts)

      assert [
               %{
                 name: "topic1",
                 partition_indexes: [0]
               },
               %{
                 name: "topic2",
                 partition_indexes: [0, 1]
               }
             ] = result
    end

    test "builds topics with single topic and multiple partitions" do
      opts = [
        topics: [
          {"my-topic",
           [
             %{partition_num: 0},
             %{partition_num: 1},
             %{partition_num: 2}
           ]}
        ]
      ]

      result = RequestHelpers.build_topics(opts)

      assert [
               %{
                 name: "my-topic",
                 partition_indexes: [0, 1, 2]
               }
             ] = result
    end

    test "builds topics with single topic and single partition" do
      opts = [
        topics: [
          {"simple-topic", [%{partition_num: 0}]}
        ]
      ]

      result = RequestHelpers.build_topics(opts)

      assert [
               %{
                 name: "simple-topic",
                 partition_indexes: [0]
               }
             ] = result
    end

    test "handles empty partitions list" do
      opts = [
        topics: [
          {"empty-topic", []}
        ]
      ]

      result = RequestHelpers.build_topics(opts)

      assert [
               %{
                 name: "empty-topic",
                 partition_indexes: []
               }
             ] = result
    end

    test "raises when topics key is missing" do
      opts = [group_id: "test-group"]

      assert_raise KeyError, fn ->
        RequestHelpers.build_topics(opts)
      end
    end

    test "handles topics with various partition numbers" do
      opts = [
        topics: [
          {"topic-a", [%{partition_num: 10}, %{partition_num: 20}]},
          {"topic-b", [%{partition_num: 99}]}
        ]
      ]

      result = RequestHelpers.build_topics(opts)

      assert [
               %{name: "topic-a", partition_indexes: [10, 20]},
               %{name: "topic-b", partition_indexes: [99]}
             ] = result
    end
  end

  describe "V0 Request implementation" do
    test "builds request with single topic and partition" do
      request = %Kayrock.OffsetFetch.V0.Request{}

      opts = [
        group_id: "test-group",
        topics: [
          {"test-topic", [%{partition_num: 0}]}
        ]
      ]

      result = OffsetFetch.Request.build_request(request, opts)

      assert result == %Kayrock.OffsetFetch.V0.Request{
               client_id: nil,
               correlation_id: nil,
               group_id: "test-group",
               topics: [
                 %{
                   name: "test-topic",
                   partition_indexes: [0]
                 }
               ]
             }
    end

    test "builds request with multiple partitions" do
      request = %Kayrock.OffsetFetch.V0.Request{}

      opts = [
        group_id: "consumer-group",
        topics: [
          {"topic-1", [%{partition_num: 0}, %{partition_num: 1}, %{partition_num: 2}]}
        ]
      ]

      result = OffsetFetch.Request.build_request(request, opts)

      assert result == %Kayrock.OffsetFetch.V0.Request{
               client_id: nil,
               correlation_id: nil,
               group_id: "consumer-group",
               topics: [
                 %{
                   name: "topic-1",
                   partition_indexes: [0, 1, 2]
                 }
               ]
             }
    end

    test "builds request with multiple topics" do
      request = %Kayrock.OffsetFetch.V0.Request{}

      opts = [
        group_id: "multi-topic-group",
        topics: [
          {"topic-1", [%{partition_num: 0}]},
          {"topic-2", [%{partition_num: 0}, %{partition_num: 1}]}
        ]
      ]

      result = OffsetFetch.Request.build_request(request, opts)

      assert result == %Kayrock.OffsetFetch.V0.Request{
               client_id: nil,
               correlation_id: nil,
               group_id: "multi-topic-group",
               topics: [
                 %{
                   name: "topic-1",
                   partition_indexes: [0]
                 },
                 %{
                   name: "topic-2",
                   partition_indexes: [0, 1]
                 }
               ]
             }
    end

    test "builds request with empty topics list" do
      request = %Kayrock.OffsetFetch.V0.Request{}

      opts = [
        group_id: "empty-group",
        topics: []
      ]

      result = OffsetFetch.Request.build_request(request, opts)

      assert result == %Kayrock.OffsetFetch.V0.Request{
               client_id: nil,
               correlation_id: nil,
               group_id: "empty-group",
               topics: []
             }
    end
  end

  describe "V1 Request implementation" do
    test "builds request with single topic and partition" do
      request = %Kayrock.OffsetFetch.V1.Request{}

      opts = [
        group_id: "coordinator-group",
        topics: [
          {"kafka-topic", [%{partition_num: 0}]}
        ]
      ]

      result = OffsetFetch.Request.build_request(request, opts)

      assert result == %Kayrock.OffsetFetch.V1.Request{
               client_id: nil,
               correlation_id: nil,
               group_id: "coordinator-group",
               topics: [
                 %{
                   name: "kafka-topic",
                   partition_indexes: [0]
                 }
               ]
             }
    end

    test "builds request with multiple partitions" do
      request = %Kayrock.OffsetFetch.V1.Request{}

      opts = [
        group_id: "v1-group",
        topics: [
          {"topic-1", [%{partition_num: 0}, %{partition_num: 1}, %{partition_num: 2}]}
        ]
      ]

      result = OffsetFetch.Request.build_request(request, opts)

      assert result == %Kayrock.OffsetFetch.V1.Request{
               client_id: nil,
               correlation_id: nil,
               group_id: "v1-group",
               topics: [
                 %{
                   name: "topic-1",
                   partition_indexes: [0, 1, 2]
                 }
               ]
             }
    end

    test "builds request with multiple topics" do
      request = %Kayrock.OffsetFetch.V1.Request{}

      opts = [
        group_id: "multi-v1-group",
        topics: [
          {"topic-a", [%{partition_num: 0}]},
          {"topic-b", [%{partition_num: 0}, %{partition_num: 1}]},
          {"topic-c", [%{partition_num: 0}]}
        ]
      ]

      result = OffsetFetch.Request.build_request(request, opts)

      assert result == %Kayrock.OffsetFetch.V1.Request{
               client_id: nil,
               correlation_id: nil,
               group_id: "multi-v1-group",
               topics: [
                 %{
                   name: "topic-a",
                   partition_indexes: [0]
                 },
                 %{
                   name: "topic-b",
                   partition_indexes: [0, 1]
                 },
                 %{
                   name: "topic-c",
                   partition_indexes: [0]
                 }
               ]
             }
    end
  end

  describe "V2 Request implementation" do
    test "builds request with single topic (same structure as V1)" do
      request = %Kayrock.OffsetFetch.V2.Request{}

      opts = [
        group_id: "v2-group",
        topics: [
          {"kafka-topic", [%{partition_num: 0}]}
        ]
      ]

      result = OffsetFetch.Request.build_request(request, opts)

      assert result == %Kayrock.OffsetFetch.V2.Request{
               client_id: nil,
               correlation_id: nil,
               group_id: "v2-group",
               topics: [
                 %{
                   name: "kafka-topic",
                   partition_indexes: [0]
                 }
               ]
             }
    end

    test "builds request with multiple partitions" do
      request = %Kayrock.OffsetFetch.V2.Request{}

      opts = [
        group_id: "v2-multi-part",
        topics: [
          {"topic-1", [%{partition_num: 0}, %{partition_num: 1}, %{partition_num: 2}]}
        ]
      ]

      result = OffsetFetch.Request.build_request(request, opts)

      assert result == %Kayrock.OffsetFetch.V2.Request{
               client_id: nil,
               correlation_id: nil,
               group_id: "v2-multi-part",
               topics: [
                 %{
                   name: "topic-1",
                   partition_indexes: [0, 1, 2]
                 }
               ]
             }
    end

    test "builds request with multiple topics" do
      request = %Kayrock.OffsetFetch.V2.Request{}

      opts = [
        group_id: "v2-multi-topic",
        topics: [
          {"topic-a", [%{partition_num: 0}]},
          {"topic-b", [%{partition_num: 0}, %{partition_num: 1}]}
        ]
      ]

      result = OffsetFetch.Request.build_request(request, opts)

      assert length(result.topics) == 2
      assert Enum.at(result.topics, 0).name == "topic-a"
      assert Enum.at(result.topics, 1).name == "topic-b"
      assert Enum.at(result.topics, 1).partition_indexes == [0, 1]
    end

    test "can serialize the built request" do
      request = %Kayrock.OffsetFetch.V2.Request{}

      opts = [
        group_id: "serialize-v2",
        topics: [
          {"topic-1", [%{partition_num: 0}]}
        ]
      ]

      result = OffsetFetch.Request.build_request(request, opts)

      result_with_client_data = %{result | client_id: "test-client", correlation_id: 1}
      assert Kayrock.OffsetFetch.V2.Request.serialize(result_with_client_data)
    end
  end

  describe "V3 Request implementation" do
    test "builds request with single topic (same structure as V1/V2)" do
      request = %Kayrock.OffsetFetch.V3.Request{}

      opts = [
        group_id: "v3-group",
        topics: [
          {"kafka-topic", [%{partition_num: 0}]}
        ]
      ]

      result = OffsetFetch.Request.build_request(request, opts)

      assert result == %Kayrock.OffsetFetch.V3.Request{
               client_id: nil,
               correlation_id: nil,
               group_id: "v3-group",
               topics: [
                 %{
                   name: "kafka-topic",
                   partition_indexes: [0]
                 }
               ]
             }
    end

    test "builds request with multiple partitions" do
      request = %Kayrock.OffsetFetch.V3.Request{}

      opts = [
        group_id: "v3-multi-part",
        topics: [
          {"topic-1", [%{partition_num: 0}, %{partition_num: 1}, %{partition_num: 2}]}
        ]
      ]

      result = OffsetFetch.Request.build_request(request, opts)

      assert result == %Kayrock.OffsetFetch.V3.Request{
               client_id: nil,
               correlation_id: nil,
               group_id: "v3-multi-part",
               topics: [
                 %{
                   name: "topic-1",
                   partition_indexes: [0, 1, 2]
                 }
               ]
             }
    end

    test "builds request with multiple topics" do
      request = %Kayrock.OffsetFetch.V3.Request{}

      opts = [
        group_id: "v3-multi-topic",
        topics: [
          {"topic-a", [%{partition_num: 0}]},
          {"topic-b", [%{partition_num: 0}, %{partition_num: 1}]},
          {"topic-c", [%{partition_num: 2}]}
        ]
      ]

      result = OffsetFetch.Request.build_request(request, opts)

      assert length(result.topics) == 3
      assert Enum.at(result.topics, 0).name == "topic-a"
      assert Enum.at(result.topics, 1).name == "topic-b"
      assert Enum.at(result.topics, 2).name == "topic-c"
    end

    test "builds request with empty topics list" do
      request = %Kayrock.OffsetFetch.V3.Request{}

      opts = [
        group_id: "empty-v3",
        topics: []
      ]

      result = OffsetFetch.Request.build_request(request, opts)

      assert result == %Kayrock.OffsetFetch.V3.Request{
               client_id: nil,
               correlation_id: nil,
               group_id: "empty-v3",
               topics: []
             }
    end

    test "can serialize the built request" do
      request = %Kayrock.OffsetFetch.V3.Request{}

      opts = [
        group_id: "serialize-v3",
        topics: [
          {"topic-1", [%{partition_num: 0}]}
        ]
      ]

      result = OffsetFetch.Request.build_request(request, opts)

      result_with_client_data = %{result | client_id: "test-client", correlation_id: 1}
      assert Kayrock.OffsetFetch.V3.Request.serialize(result_with_client_data)
    end
  end

  describe "V4 Request implementation" do
    test "builds request with single topic (same structure as V3)" do
      request = %Kayrock.OffsetFetch.V4.Request{}

      opts = [
        group_id: "v4-group",
        topics: [
          {"kafka-topic", [%{partition_num: 0}]}
        ]
      ]

      result = OffsetFetch.Request.build_request(request, opts)

      assert result == %Kayrock.OffsetFetch.V4.Request{
               client_id: nil,
               correlation_id: nil,
               group_id: "v4-group",
               topics: [
                 %{
                   name: "kafka-topic",
                   partition_indexes: [0]
                 }
               ]
             }
    end

    test "builds request with multiple partitions" do
      request = %Kayrock.OffsetFetch.V4.Request{}

      opts = [
        group_id: "v4-multi-part",
        topics: [
          {"topic-1", [%{partition_num: 0}, %{partition_num: 1}, %{partition_num: 2}]}
        ]
      ]

      result = OffsetFetch.Request.build_request(request, opts)

      assert result == %Kayrock.OffsetFetch.V4.Request{
               client_id: nil,
               correlation_id: nil,
               group_id: "v4-multi-part",
               topics: [
                 %{
                   name: "topic-1",
                   partition_indexes: [0, 1, 2]
                 }
               ]
             }
    end

    test "builds request with multiple topics" do
      request = %Kayrock.OffsetFetch.V4.Request{}

      opts = [
        group_id: "v4-multi-topic",
        topics: [
          {"topic-a", [%{partition_num: 0}]},
          {"topic-b", [%{partition_num: 0}, %{partition_num: 1}]}
        ]
      ]

      result = OffsetFetch.Request.build_request(request, opts)

      assert length(result.topics) == 2
      assert Enum.at(result.topics, 0).name == "topic-a"
      assert Enum.at(result.topics, 1).name == "topic-b"
      assert Enum.at(result.topics, 1).partition_indexes == [0, 1]
    end

    test "builds request with empty topics list" do
      request = %Kayrock.OffsetFetch.V4.Request{}

      opts = [
        group_id: "empty-v4",
        topics: []
      ]

      result = OffsetFetch.Request.build_request(request, opts)

      assert result == %Kayrock.OffsetFetch.V4.Request{
               client_id: nil,
               correlation_id: nil,
               group_id: "empty-v4",
               topics: []
             }
    end

    test "can serialize the built request" do
      request = %Kayrock.OffsetFetch.V4.Request{}

      opts = [
        group_id: "serialize-v4",
        topics: [
          {"topic-1", [%{partition_num: 0}]}
        ]
      ]

      result = OffsetFetch.Request.build_request(request, opts)

      result_with_client_data = %{result | client_id: "test-client", correlation_id: 1}
      assert Kayrock.OffsetFetch.V4.Request.serialize(result_with_client_data)
    end
  end

  describe "V5 Request implementation" do
    test "builds request with single topic (same structure as V3/V4)" do
      request = %Kayrock.OffsetFetch.V5.Request{}

      opts = [
        group_id: "v5-group",
        topics: [
          {"kafka-topic", [%{partition_num: 0}]}
        ]
      ]

      result = OffsetFetch.Request.build_request(request, opts)

      assert result == %Kayrock.OffsetFetch.V5.Request{
               client_id: nil,
               correlation_id: nil,
               group_id: "v5-group",
               topics: [
                 %{
                   name: "kafka-topic",
                   partition_indexes: [0]
                 }
               ]
             }
    end

    test "builds request with multiple partitions" do
      request = %Kayrock.OffsetFetch.V5.Request{}

      opts = [
        group_id: "v5-multi-part",
        topics: [
          {"topic-1", [%{partition_num: 0}, %{partition_num: 1}, %{partition_num: 2}]}
        ]
      ]

      result = OffsetFetch.Request.build_request(request, opts)

      assert result == %Kayrock.OffsetFetch.V5.Request{
               client_id: nil,
               correlation_id: nil,
               group_id: "v5-multi-part",
               topics: [
                 %{
                   name: "topic-1",
                   partition_indexes: [0, 1, 2]
                 }
               ]
             }
    end

    test "builds request with multiple topics" do
      request = %Kayrock.OffsetFetch.V5.Request{}

      opts = [
        group_id: "v5-multi-topic",
        topics: [
          {"topic-a", [%{partition_num: 0}]},
          {"topic-b", [%{partition_num: 0}, %{partition_num: 1}]},
          {"topic-c", [%{partition_num: 2}]}
        ]
      ]

      result = OffsetFetch.Request.build_request(request, opts)

      assert length(result.topics) == 3
      assert Enum.at(result.topics, 0).name == "topic-a"
      assert Enum.at(result.topics, 1).name == "topic-b"
      assert Enum.at(result.topics, 2).name == "topic-c"
    end

    test "builds request with empty topics list" do
      request = %Kayrock.OffsetFetch.V5.Request{}

      opts = [
        group_id: "empty-v5",
        topics: []
      ]

      result = OffsetFetch.Request.build_request(request, opts)

      assert result == %Kayrock.OffsetFetch.V5.Request{
               client_id: nil,
               correlation_id: nil,
               group_id: "empty-v5",
               topics: []
             }
    end

    test "can serialize the built request" do
      request = %Kayrock.OffsetFetch.V5.Request{}

      opts = [
        group_id: "serialize-v5",
        topics: [
          {"topic-1", [%{partition_num: 0}]}
        ]
      ]

      result = OffsetFetch.Request.build_request(request, opts)

      result_with_client_data = %{result | client_id: "test-client", correlation_id: 1}
      assert Kayrock.OffsetFetch.V5.Request.serialize(result_with_client_data)
    end
  end

  describe "V6 Request implementation (flexible version)" do
    test "builds request with single topic" do
      request = %Kayrock.OffsetFetch.V6.Request{}

      opts = [
        group_id: "v6-group",
        topics: [
          {"kafka-topic", [%{partition_num: 0}]}
        ]
      ]

      result = OffsetFetch.Request.build_request(request, opts)

      assert result.group_id == "v6-group"
      assert length(result.topics) == 1
      assert hd(result.topics).name == "kafka-topic"
      assert hd(result.topics).partition_indexes == [0]
    end

    test "builds request with multiple partitions" do
      request = %Kayrock.OffsetFetch.V6.Request{}

      opts = [
        group_id: "v6-multi-part",
        topics: [
          {"topic-1", [%{partition_num: 0}, %{partition_num: 1}, %{partition_num: 2}]}
        ]
      ]

      result = OffsetFetch.Request.build_request(request, opts)

      assert result.group_id == "v6-multi-part"
      assert hd(result.topics).partition_indexes == [0, 1, 2]
    end

    test "builds request with multiple topics" do
      request = %Kayrock.OffsetFetch.V6.Request{}

      opts = [
        group_id: "v6-multi-topic",
        topics: [
          {"topic-a", [%{partition_num: 0}]},
          {"topic-b", [%{partition_num: 0}, %{partition_num: 1}]}
        ]
      ]

      result = OffsetFetch.Request.build_request(request, opts)

      assert length(result.topics) == 2
      assert Enum.at(result.topics, 0).name == "topic-a"
      assert Enum.at(result.topics, 1).name == "topic-b"
    end

    test "preserves tagged_fields from template (default empty list)" do
      request = %Kayrock.OffsetFetch.V6.Request{}

      opts = [
        group_id: "v6-tagged",
        topics: [
          {"topic-1", [%{partition_num: 0}]}
        ]
      ]

      result = OffsetFetch.Request.build_request(request, opts)

      # V6 struct has tagged_fields key, defaults to empty list
      assert Map.has_key?(result, :tagged_fields)
      assert result.tagged_fields == []
    end

    test "preserves non-empty tagged_fields from template" do
      request = %Kayrock.OffsetFetch.V6.Request{tagged_fields: [{0, <<1, 2, 3>>}]}

      opts = [
        group_id: "v6-tagged-nonempty",
        topics: [
          {"topic-1", [%{partition_num: 0}]}
        ]
      ]

      result = OffsetFetch.Request.build_request(request, opts)

      assert result.tagged_fields == [{0, <<1, 2, 3>>}]
      assert result.group_id == "v6-tagged-nonempty"
      assert length(result.topics) == 1
    end

    test "builds request with empty topics list" do
      request = %Kayrock.OffsetFetch.V6.Request{}

      opts = [
        group_id: "empty-v6",
        topics: []
      ]

      result = OffsetFetch.Request.build_request(request, opts)

      assert result.group_id == "empty-v6"
      assert result.topics == []
    end

    test "can serialize the built request" do
      request = %Kayrock.OffsetFetch.V6.Request{}

      opts = [
        group_id: "serialize-v6",
        topics: [
          {"topic-1", [%{partition_num: 0}]}
        ]
      ]

      result = OffsetFetch.Request.build_request(request, opts)

      result_with_client_data = %{result | client_id: "test-client", correlation_id: 1}
      assert Kayrock.OffsetFetch.V6.Request.serialize(result_with_client_data)
    end
  end

  describe "Any Request fallback implementation" do
    test "handles V4-like struct via Any fallback using plain map" do
      # Simulate an unknown future version with same structure as V4
      request = %{group_id: nil, topics: [], correlation_id: nil, client_id: nil}

      opts = [
        group_id: "any-group",
        topics: [
          {"any-topic", [%{partition_num: 0}]}
        ]
      ]

      result = OffsetFetch.Request.build_request(request, opts)

      assert result.group_id == "any-group"
      assert length(result.topics) == 1
      assert hd(result.topics).name == "any-topic"
      assert hd(result.topics).partition_indexes == [0]
    end

    test "handles struct with tagged_fields via Any fallback using plain map" do
      # Simulate an unknown flexible version
      request = %{group_id: nil, topics: [], tagged_fields: [], correlation_id: nil, client_id: nil}

      opts = [
        group_id: "any-flex-group",
        topics: [
          {"flex-topic", [%{partition_num: 0}, %{partition_num: 1}]}
        ]
      ]

      result = OffsetFetch.Request.build_request(request, opts)

      assert result.group_id == "any-flex-group"
      assert length(result.topics) == 1
      assert hd(result.topics).partition_indexes == [0, 1]
    end

    test "handles multiple topics via Any fallback using plain map" do
      request = %{group_id: nil, topics: [], correlation_id: nil, client_id: nil}

      opts = [
        group_id: "any-multi",
        topics: [
          {"topic-a", [%{partition_num: 0}]},
          {"topic-b", [%{partition_num: 1}, %{partition_num: 2}]}
        ]
      ]

      result = OffsetFetch.Request.build_request(request, opts)

      assert result.group_id == "any-multi"
      assert length(result.topics) == 2
    end
  end
end
