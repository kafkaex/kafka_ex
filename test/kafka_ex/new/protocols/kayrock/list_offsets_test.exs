defmodule KafkaEx.New.Protocols.ListOffsetsTest do
  use ExUnit.Case, async: true

  alias KafkaEx.New.Protocols.ListOffsets

  alias Kayrock.ListOffsets.V0
  alias Kayrock.ListOffsets.V1
  alias Kayrock.ListOffsets.V2

  describe "build_request/2" do
    test "api version 0 - returns Kayrock request for single topic partition" do
      topic_partitions = [{"topic1", [1]}]

      assert %V0.Request{
               replica_id: -1,
               topics: [
                 %{
                   topic: "topic1",
                   partitions: [
                     %{partition: 1, timestamp: -1, max_num_offsets: 1}
                   ]
                 }
               ]
             } ==
               ListOffsets.Request.build_request(
                 %V0.Request{},
                 topic_partitions
               )
    end

    test "api version 0 - returns Kayrock request for single topic multiple partitions" do
      topic_partitions = [{"topic1", [1, 3]}]

      assert %V0.Request{
               replica_id: -1,
               topics: [
                 %{
                   topic: "topic1",
                   partitions: [
                     %{partition: 1, timestamp: -1, max_num_offsets: 1},
                     %{partition: 3, timestamp: -1, max_num_offsets: 1}
                   ]
                 }
               ]
             } ==
               ListOffsets.Request.build_request(
                 %V0.Request{},
                 topic_partitions
               )
    end

    test "api version 0 - returns Kayrock request for multiple topics multiple partitions" do
      topic_partitions = [{"topic1", [1, 2]}, {"topic2", [3, 4]}]

      assert %V0.Request{
               replica_id: -1,
               topics: [
                 %{
                   topic: "topic1",
                   partitions: [
                     %{partition: 1, timestamp: -1, max_num_offsets: 1},
                     %{partition: 2, timestamp: -1, max_num_offsets: 1}
                   ]
                 },
                 %{
                   topic: "topic2",
                   partitions: [
                     %{partition: 3, timestamp: -1, max_num_offsets: 1},
                     %{partition: 4, timestamp: -1, max_num_offsets: 1}
                   ]
                 }
               ]
             } ==
               ListOffsets.Request.build_request(
                 %V0.Request{},
                 topic_partitions
               )
    end

    test "api version 1 - returns Kayrock request for single topic partition" do
      topic_partitions = [{"topic1", [1]}]

      assert %V1.Request{
               replica_id: -1,
               topics: [
                 %{
                   topic: "topic1",
                   partitions: [%{partition: 1, timestamp: -1}]
                 }
               ]
             } ==
               ListOffsets.Request.build_request(
                 %V1.Request{},
                 topic_partitions
               )
    end

    test "api version 1 - returns Kayrock request for single topic multiple partitions" do
      topic_partitions = [{"topic1", [1, 3]}]

      assert %V1.Request{
               replica_id: -1,
               topics: [
                 %{
                   topic: "topic1",
                   partitions: [
                     %{partition: 1, timestamp: -1},
                     %{partition: 3, timestamp: -1}
                   ]
                 }
               ]
             } ==
               ListOffsets.Request.build_request(
                 %V1.Request{},
                 topic_partitions
               )
    end

    test "api version 1 - returns kayrock request for multiple topics multiple partitions" do
      topic_partitions = [{"topic1", [1, 2]}, {"topic2", [3, 4]}]

      assert %V1.Request{
               replica_id: -1,
               topics: [
                 %{
                   topic: "topic1",
                   partitions: [
                     %{partition: 1, timestamp: -1},
                     %{partition: 2, timestamp: -1}
                   ]
                 },
                 %{
                   topic: "topic2",
                   partitions: [
                     %{partition: 3, timestamp: -1},
                     %{partition: 4, timestamp: -1}
                   ]
                 }
               ]
             } ==
               ListOffsets.Request.build_request(
                 %V1.Request{},
                 topic_partitions
               )
    end

    test "api version 2 - returns kayrock request for single topic partition" do
      topic_partitions = [{"topic1", [1]}]

      assert %V2.Request{
               replica_id: -1,
               isolation_level: 1,
               topics: [
                 %{
                   topic: "topic1",
                   partitions: [%{partition: 1, timestamp: -1}]
                 }
               ]
             } ==
               ListOffsets.Request.build_request(
                 %V2.Request{},
                 topic_partitions
               )
    end

    test "api version 2 - add optional isolation level" do
      topic_partitions = [{"topic1", [1]}]

      %V2.Request{
        replica_id: -1,
        isolation_level: isolation_level,
        topics: [
          %{
            topic: "topic1",
            partitions: [%{partition: 1, timestamp: -1}]
          }
        ]
      } =
        ListOffsets.Request.build_request(%V2.Request{}, topic_partitions,
          isolation_level: 0
        )

      assert isolation_level == 0
    end

    test "api version 2 - returns Kayrock request for single topic multiple partitions" do
      topic_partitions = [{"topic1", [1, 3]}]

      assert %V2.Request{
               replica_id: -1,
               isolation_level: 1,
               topics: [
                 %{
                   topic: "topic1",
                   partitions: [
                     %{partition: 1, timestamp: -1},
                     %{partition: 3, timestamp: -1}
                   ]
                 }
               ]
             } ==
               ListOffsets.Request.build_request(
                 %V2.Request{},
                 topic_partitions
               )
    end

    test "api version 2 - returns Kayrock request for multiple topics multiple partitions" do
      topic_partitions = [{"topic1", [1, 2]}, {"topic2", [3, 4]}]

      assert %V2.Request{
               replica_id: -1,
               isolation_level: 1,
               topics: [
                 %{
                   topic: "topic1",
                   partitions: [
                     %{partition: 1, timestamp: -1},
                     %{partition: 2, timestamp: -1}
                   ]
                 },
                 %{
                   topic: "topic2",
                   partitions: [
                     %{partition: 3, timestamp: -1},
                     %{partition: 4, timestamp: -1}
                   ]
                 }
               ]
             } ==
               ListOffsets.Request.build_request(
                 %V2.Request{},
                 topic_partitions
               )
    end
  end

  describe "get_partition_offset/3" do
    @response_v0 %V0.Response{
      responses: [
        %{
          topic: "topic1",
          partition_responses: [
            %{partition: 1, error_code: 0, offsets: [100]},
            %{partition: 2, error_code: 1, offsets: []}
          ]
        },
        %{
          topic: "topic2",
          partition_responses: [
            %{partition: 3, error_code: 0, offsets: [200]},
            %{partition: 4, error_code: 2, offsets: []}
          ]
        }
      ]
    }

    @response_v1 %V1.Response{
      responses: [
        %{
          topic: "topic1",
          partition_responses: [
            %{partition: 1, error_code: 0, offset: 100},
            %{partition: 2, error_code: 1, offset: nil}
          ]
        },
        %{
          topic: "topic2",
          partition_responses: [
            %{partition: 3, error_code: 0, offset: 200},
            %{partition: 4, error_code: 2, offset: nil}
          ]
        }
      ]
    }

    @response_v2 %V1.Response{
      responses: [
        %{
          topic: "topic1",
          partition_responses: [
            %{partition: 1, error_code: 0, offset: 100},
            %{partition: 2, error_code: 1, offset: nil}
          ]
        },
        %{
          topic: "topic2",
          partition_responses: [
            %{partition: 3, error_code: 0, offset: 200},
            %{partition: 4, error_code: 2, offset: nil}
          ]
        }
      ]
    }

    for {api_version, response} <- [
          {"1", Macro.escape(@response_v0)},
          {"2", Macro.escape(@response_v1)},
          {"3", Macro.escape(@response_v2)}
        ] do
      test "api version #{api_version} - returns offset for single partition" do
        assert {:ok, 100} ==
                 ListOffsets.Response.get_partition_offset(
                   unquote(response),
                   "topic1",
                   1
                 )
      end

      test "api version #{api_version} - returns kayrock error code" do
        assert {:error, :offset_out_of_range} ==
                 ListOffsets.Response.get_partition_offset(
                   unquote(response),
                   "topic1",
                   2
                 )
      end

      test "api version #{api_version} - returns :not found when partition are is found in response" do
        assert {:error, :partition_not_found} ==
                 ListOffsets.Response.get_partition_offset(
                   unquote(response),
                   "topic2",
                   1
                 )
      end

      test "api version #{api_version} - returns :not found when topic are is found in response" do
        assert {:error, :topic_not_found} ==
                 ListOffsets.Response.get_partition_offset(
                   unquote(response),
                   "topic3",
                   2
                 )
      end
    end
  end
end
