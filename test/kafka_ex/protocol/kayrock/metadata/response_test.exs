defmodule KafkaEx.Protocol.Kayrock.Metadata.ResponseTest do
  use ExUnit.Case, async: true

  alias KafkaEx.Protocol.Kayrock.Metadata.Response, as: MetadataResponse
  alias KafkaEx.Cluster.{Broker, ClusterMetadata, Topic}
  alias Kayrock.Metadata.V0
  alias Kayrock.Metadata.V1
  alias Kayrock.Metadata.V2
  alias Kayrock.Metadata.V3
  alias Kayrock.Metadata.V4
  alias Kayrock.Metadata.V5
  alias Kayrock.Metadata.V6
  alias Kayrock.Metadata.V7
  alias Kayrock.Metadata.V8
  alias Kayrock.Metadata.V9

  # ---------------------------------------------------------------------------
  # Shared test data helpers
  # ---------------------------------------------------------------------------

  defp base_broker(opts \\ []) do
    %{
      node_id: Keyword.get(opts, :node_id, 1),
      host: Keyword.get(opts, :host, "broker1.example.com"),
      port: Keyword.get(opts, :port, 9092),
      rack: Keyword.get(opts, :rack, nil)
    }
  end

  defp base_partition(opts \\ []) do
    %{
      error_code: 0,
      partition_index: Keyword.get(opts, :partition_index, 0),
      leader_id: Keyword.get(opts, :leader_id, 1),
      replica_nodes: Keyword.get(opts, :replica_nodes, [1, 2, 3]),
      isr_nodes: Keyword.get(opts, :isr_nodes, [1, 2])
    }
  end

  defp base_topic(opts \\ []) do
    %{
      error_code: 0,
      name: Keyword.get(opts, :name, "test-topic"),
      is_internal: Keyword.get(opts, :is_internal, false),
      partitions: Keyword.get(opts, :partitions, [base_partition()])
    }
  end

  # ---------------------------------------------------------------------------
  # V0
  # ---------------------------------------------------------------------------

  describe "parse_response/1 for V0" do
    test "parses response with single broker and topic" do
      response = %V0.Response{
        brokers: [
          %{node_id: 1, host: "broker1.example.com", port: 9092}
        ],
        topics: [
          %{
            error_code: 0,
            name: "test-topic",
            partitions: [
              %{
                error_code: 0,
                partition_index: 0,
                leader_id: 1,
                replica_nodes: [1, 2, 3],
                isr_nodes: [1, 2]
              }
            ]
          }
        ]
      }

      {:ok, cluster_metadata} = MetadataResponse.parse_response(response)

      assert %ClusterMetadata{} = cluster_metadata
      assert map_size(cluster_metadata.brokers) == 1
      assert map_size(cluster_metadata.topics) == 1
      assert cluster_metadata.controller_id == nil

      broker = cluster_metadata.brokers[1]
      assert %Broker{node_id: 1, host: "broker1.example.com", port: 9092} = broker
      assert broker.rack == nil

      topic = cluster_metadata.topics["test-topic"]
      assert %Topic{name: "test-topic", is_internal: false} = topic
      assert length(topic.partitions) == 1
      assert topic.partition_leaders == %{0 => 1}
    end

    test "parses response with multiple brokers" do
      response = %V0.Response{
        brokers: [
          %{node_id: 1, host: "broker1.example.com", port: 9092},
          %{node_id: 2, host: "broker2.example.com", port: 9092},
          %{node_id: 3, host: "broker3.example.com", port: 9092}
        ],
        topics: []
      }

      {:ok, cluster_metadata} = MetadataResponse.parse_response(response)

      assert map_size(cluster_metadata.brokers) == 3
      assert cluster_metadata.brokers[1].node_id == 1
      assert cluster_metadata.brokers[2].node_id == 2
      assert cluster_metadata.brokers[3].node_id == 3
    end

    test "parses response with multiple topics and partitions" do
      response = %V0.Response{
        brokers: [
          %{node_id: 1, host: "broker1.example.com", port: 9092}
        ],
        topics: [
          %{
            error_code: 0,
            name: "topic1",
            partitions: [
              %{error_code: 0, partition_index: 0, leader_id: 1, replica_nodes: [1], isr_nodes: [1]},
              %{error_code: 0, partition_index: 1, leader_id: 1, replica_nodes: [1], isr_nodes: [1]}
            ]
          },
          %{
            error_code: 0,
            name: "topic2",
            partitions: [
              %{error_code: 0, partition_index: 0, leader_id: 1, replica_nodes: [1], isr_nodes: [1]}
            ]
          }
        ]
      }

      {:ok, cluster_metadata} = MetadataResponse.parse_response(response)

      assert map_size(cluster_metadata.topics) == 2
      assert cluster_metadata.topics["topic1"].name == "topic1"
      assert cluster_metadata.topics["topic2"].name == "topic2"
      assert length(cluster_metadata.topics["topic1"].partitions) == 2
      assert length(cluster_metadata.topics["topic2"].partitions) == 1
    end

    test "filters out topics with errors" do
      response = %V0.Response{
        brokers: [
          %{node_id: 1, host: "broker1.example.com", port: 9092}
        ],
        topics: [
          %{
            error_code: 0,
            name: "good-topic",
            partitions: [
              %{error_code: 0, partition_index: 0, leader_id: 1, replica_nodes: [1], isr_nodes: [1]}
            ]
          },
          %{
            error_code: 3,
            # UNKNOWN_TOPIC_OR_PARTITION
            name: "bad-topic",
            partitions: []
          }
        ]
      }

      {:ok, cluster_metadata} = MetadataResponse.parse_response(response)

      assert map_size(cluster_metadata.topics) == 1
      assert cluster_metadata.topics["good-topic"]
      refute cluster_metadata.topics["bad-topic"]
    end

    test "filters out partitions with errors" do
      response = %V0.Response{
        brokers: [
          %{node_id: 1, host: "broker1.example.com", port: 9092}
        ],
        topics: [
          %{
            error_code: 0,
            name: "test-topic",
            partitions: [
              %{error_code: 0, partition_index: 0, leader_id: 1, replica_nodes: [1], isr_nodes: [1]},
              %{error_code: 5, partition_index: 1, leader_id: -1, replica_nodes: [], isr_nodes: []}
            ]
          }
        ]
      }

      {:ok, cluster_metadata} = MetadataResponse.parse_response(response)

      topic = cluster_metadata.topics["test-topic"]
      assert length(topic.partitions) == 1
      assert Enum.at(topic.partitions, 0).partition_id == 0
    end
  end

  # ---------------------------------------------------------------------------
  # V1
  # ---------------------------------------------------------------------------

  describe "parse_response/1 for V1" do
    test "parses response with controller_id" do
      response = %V1.Response{
        brokers: [
          %{node_id: 1, host: "broker1.example.com", port: 9092, rack: nil},
          %{node_id: 2, host: "broker2.example.com", port: 9092, rack: "rack-1"}
        ],
        controller_id: 2,
        topics: []
      }

      {:ok, cluster_metadata} = MetadataResponse.parse_response(response)

      assert cluster_metadata.controller_id == 2
      assert cluster_metadata.brokers[1].rack == nil
      assert cluster_metadata.brokers[2].rack == "rack-1"
    end

    test "parses response with is_internal flag" do
      response = %V1.Response{
        brokers: [
          %{node_id: 1, host: "broker1.example.com", port: 9092, rack: nil}
        ],
        controller_id: 1,
        topics: [
          %{
            error_code: 0,
            name: "user-topic",
            is_internal: false,
            partitions: [
              %{error_code: 0, partition_index: 0, leader_id: 1, replica_nodes: [1], isr_nodes: [1]}
            ]
          },
          %{
            error_code: 0,
            name: "__consumer_offsets",
            is_internal: true,
            partitions: [
              %{error_code: 0, partition_index: 0, leader_id: 1, replica_nodes: [1], isr_nodes: [1]}
            ]
          }
        ]
      }

      {:ok, cluster_metadata} = MetadataResponse.parse_response(response)

      assert cluster_metadata.topics["user-topic"].is_internal == false
      assert cluster_metadata.topics["__consumer_offsets"].is_internal == true
    end

    test "parses response with rack information" do
      response = %V1.Response{
        brokers: [
          %{node_id: 1, host: "broker1.example.com", port: 9092, rack: "us-east-1a"},
          %{node_id: 2, host: "broker2.example.com", port: 9092, rack: "us-east-1b"},
          %{node_id: 3, host: "broker3.example.com", port: 9092, rack: nil}
        ],
        controller_id: 1,
        topics: []
      }

      {:ok, cluster_metadata} = MetadataResponse.parse_response(response)

      assert cluster_metadata.brokers[1].rack == "us-east-1a"
      assert cluster_metadata.brokers[2].rack == "us-east-1b"
      assert cluster_metadata.brokers[3].rack == nil
    end
  end

  # ---------------------------------------------------------------------------
  # V2
  # ---------------------------------------------------------------------------

  describe "parse_response/1 for V2" do
    test "parses response with cluster_id" do
      response = %V2.Response{
        brokers: [
          %{node_id: 1, host: "broker1.example.com", port: 9092, rack: nil}
        ],
        cluster_id: "kafka-cluster-prod",
        controller_id: 1,
        topics: []
      }

      {:ok, cluster_metadata} = MetadataResponse.parse_response(response)

      # Note: cluster_id is currently not stored in ClusterMetadata
      # This test verifies parsing works even though cluster_id is not persisted
      assert %ClusterMetadata{} = cluster_metadata
      assert cluster_metadata.controller_id == 1
    end

    test "parses response with nil cluster_id" do
      response = %V2.Response{
        brokers: [
          %{node_id: 1, host: "broker1.example.com", port: 9092, rack: nil}
        ],
        cluster_id: nil,
        controller_id: 1,
        topics: []
      }

      {:ok, cluster_metadata} = MetadataResponse.parse_response(response)

      assert %ClusterMetadata{} = cluster_metadata
      assert cluster_metadata.controller_id == 1
    end

    test "parses response with all V2 features" do
      response = %V2.Response{
        brokers: [
          %{node_id: 1, host: "broker1.example.com", port: 9092, rack: "rack-a"},
          %{node_id: 2, host: "broker2.example.com", port: 9092, rack: "rack-b"}
        ],
        cluster_id: "prod-cluster-1",
        controller_id: 2,
        topics: [
          %{
            error_code: 0,
            name: "events",
            is_internal: false,
            partitions: [
              %{error_code: 0, partition_index: 0, leader_id: 1, replica_nodes: [1, 2], isr_nodes: [1, 2]},
              %{error_code: 0, partition_index: 1, leader_id: 2, replica_nodes: [2, 1], isr_nodes: [2, 1]}
            ]
          }
        ]
      }

      {:ok, cluster_metadata} = MetadataResponse.parse_response(response)

      assert map_size(cluster_metadata.brokers) == 2
      assert cluster_metadata.controller_id == 2
      assert map_size(cluster_metadata.topics) == 1

      topic = cluster_metadata.topics["events"]
      assert topic.name == "events"
      assert topic.is_internal == false
      assert length(topic.partitions) == 2
      assert topic.partition_leaders == %{0 => 1, 1 => 2}
    end
  end

  # ---------------------------------------------------------------------------
  # V3
  # ---------------------------------------------------------------------------

  describe "parse_response/1 for V3" do
    test "parses response with throttle_time_ms and cluster_id" do
      response = %V3.Response{
        throttle_time_ms: 100,
        brokers: [base_broker(rack: "rack-a")],
        cluster_id: "cluster-v3",
        controller_id: 1,
        topics: [base_topic()]
      }

      {:ok, cluster_metadata} = MetadataResponse.parse_response(response)

      assert %ClusterMetadata{} = cluster_metadata
      assert cluster_metadata.controller_id == 1
      assert map_size(cluster_metadata.brokers) == 1
      assert map_size(cluster_metadata.topics) == 1
      assert cluster_metadata.brokers[1].rack == "rack-a"
    end

    test "parses response with nil cluster_id and zero throttle" do
      response = %V3.Response{
        throttle_time_ms: 0,
        brokers: [base_broker()],
        cluster_id: nil,
        controller_id: 1,
        topics: []
      }

      {:ok, cluster_metadata} = MetadataResponse.parse_response(response)

      assert map_size(cluster_metadata.topics) == 0
      assert cluster_metadata.controller_id == 1
    end

    test "V3 response struct has throttle_time_ms not present in V2" do
      v2 = %V2.Response{}
      v3 = %V3.Response{}

      v2_keys = Map.keys(v2) -- [:__struct__]
      v3_keys = Map.keys(v3) -- [:__struct__]

      new_fields = v3_keys -- v2_keys
      assert :throttle_time_ms in new_fields
    end
  end

  # ---------------------------------------------------------------------------
  # V4
  # ---------------------------------------------------------------------------

  describe "parse_response/1 for V4" do
    test "parses response (same schema as V3)" do
      response = %V4.Response{
        throttle_time_ms: 50,
        brokers: [
          base_broker(node_id: 1, rack: "az-1"),
          base_broker(node_id: 2, host: "broker2.example.com", rack: "az-2")
        ],
        cluster_id: "cluster-v4",
        controller_id: 2,
        topics: [base_topic(name: "orders")]
      }

      {:ok, cluster_metadata} = MetadataResponse.parse_response(response)

      assert cluster_metadata.controller_id == 2
      assert map_size(cluster_metadata.brokers) == 2
      assert cluster_metadata.topics["orders"].name == "orders"
    end

    test "V4 response struct has same fields as V3" do
      v3 = %V3.Response{}
      v4 = %V4.Response{}

      v3_keys = Map.keys(v3) |> MapSet.new()
      v4_keys = Map.keys(v4) |> MapSet.new()

      assert v3_keys == v4_keys
    end
  end

  # ---------------------------------------------------------------------------
  # V5 - adds offline_replicas in partition metadata
  # ---------------------------------------------------------------------------

  describe "parse_response/1 for V5" do
    test "parses response with offline_replicas in partitions" do
      response = %V5.Response{
        throttle_time_ms: 0,
        brokers: [base_broker()],
        cluster_id: "cluster-v5",
        controller_id: 1,
        topics: [
          %{
            error_code: 0,
            name: "topic-v5",
            is_internal: false,
            partitions: [
              %{
                error_code: 0,
                partition_index: 0,
                leader_id: 1,
                replica_nodes: [1, 2, 3],
                isr_nodes: [1, 2],
                offline_replicas: [3]
              }
            ]
          }
        ]
      }

      {:ok, cluster_metadata} = MetadataResponse.parse_response(response)

      topic = cluster_metadata.topics["topic-v5"]
      assert length(topic.partitions) == 1
      partition = Enum.at(topic.partitions, 0)
      assert partition.partition_id == 0
      assert partition.leader == 1
      assert partition.replicas == [1, 2, 3]
      assert partition.isr == [1, 2]
      # offline_replicas is not in PartitionInfo struct, but parsing should not fail
    end

    test "parses response with empty offline_replicas" do
      response = %V5.Response{
        throttle_time_ms: 0,
        brokers: [base_broker()],
        cluster_id: nil,
        controller_id: 1,
        topics: [
          %{
            error_code: 0,
            name: "topic",
            is_internal: false,
            partitions: [
              %{
                error_code: 0,
                partition_index: 0,
                leader_id: 1,
                replica_nodes: [1],
                isr_nodes: [1],
                offline_replicas: []
              }
            ]
          }
        ]
      }

      {:ok, cluster_metadata} = MetadataResponse.parse_response(response)

      assert map_size(cluster_metadata.topics) == 1
    end
  end

  # ---------------------------------------------------------------------------
  # V6 - same schema as V5
  # ---------------------------------------------------------------------------

  describe "parse_response/1 for V6" do
    test "parses response (same schema as V5)" do
      response = %V6.Response{
        throttle_time_ms: 10,
        brokers: [base_broker(rack: "us-west-2a")],
        cluster_id: "cluster-v6",
        controller_id: 1,
        topics: [
          %{
            error_code: 0,
            name: "v6-topic",
            is_internal: false,
            partitions: [
              %{
                error_code: 0,
                partition_index: 0,
                leader_id: 1,
                replica_nodes: [1],
                isr_nodes: [1],
                offline_replicas: []
              }
            ]
          }
        ]
      }

      {:ok, cluster_metadata} = MetadataResponse.parse_response(response)

      assert cluster_metadata.controller_id == 1
      assert cluster_metadata.topics["v6-topic"].name == "v6-topic"
      assert cluster_metadata.brokers[1].rack == "us-west-2a"
    end

    test "V6 response struct has same fields as V5" do
      v5 = %V5.Response{}
      v6 = %V6.Response{}

      v5_keys = Map.keys(v5) |> MapSet.new()
      v6_keys = Map.keys(v6) |> MapSet.new()

      assert v5_keys == v6_keys
    end
  end

  # ---------------------------------------------------------------------------
  # V7 - adds leader_epoch in partition metadata
  # ---------------------------------------------------------------------------

  describe "parse_response/1 for V7" do
    test "parses response with leader_epoch in partitions" do
      response = %V7.Response{
        throttle_time_ms: 0,
        brokers: [base_broker()],
        cluster_id: "cluster-v7",
        controller_id: 1,
        topics: [
          %{
            error_code: 0,
            name: "topic-v7",
            is_internal: false,
            partitions: [
              %{
                error_code: 0,
                partition_index: 0,
                leader_id: 1,
                leader_epoch: 42,
                replica_nodes: [1, 2],
                isr_nodes: [1, 2],
                offline_replicas: []
              }
            ]
          }
        ]
      }

      {:ok, cluster_metadata} = MetadataResponse.parse_response(response)

      topic = cluster_metadata.topics["topic-v7"]
      assert length(topic.partitions) == 1
      partition = Enum.at(topic.partitions, 0)
      assert partition.partition_id == 0
      assert partition.leader == 1
      # leader_epoch is not in PartitionInfo struct, but parsing should not fail
    end

    test "parses response with multiple partitions having different leader_epochs" do
      response = %V7.Response{
        throttle_time_ms: 0,
        brokers: [
          base_broker(node_id: 1),
          base_broker(node_id: 2, host: "broker2.example.com")
        ],
        cluster_id: nil,
        controller_id: 1,
        topics: [
          %{
            error_code: 0,
            name: "multi-partition",
            is_internal: false,
            partitions: [
              %{
                error_code: 0,
                partition_index: 0,
                leader_id: 1,
                leader_epoch: 10,
                replica_nodes: [1, 2],
                isr_nodes: [1, 2],
                offline_replicas: []
              },
              %{
                error_code: 0,
                partition_index: 1,
                leader_id: 2,
                leader_epoch: 15,
                replica_nodes: [2, 1],
                isr_nodes: [2, 1],
                offline_replicas: []
              }
            ]
          }
        ]
      }

      {:ok, cluster_metadata} = MetadataResponse.parse_response(response)

      topic = cluster_metadata.topics["multi-partition"]
      assert length(topic.partitions) == 2
      assert topic.partition_leaders == %{0 => 1, 1 => 2}
    end
  end

  # ---------------------------------------------------------------------------
  # V8 - adds authorized_operations
  # ---------------------------------------------------------------------------

  describe "parse_response/1 for V8" do
    test "parses response with authorized_operations" do
      response = %V8.Response{
        throttle_time_ms: 0,
        brokers: [base_broker()],
        cluster_id: "cluster-v8",
        controller_id: 1,
        topics: [
          %{
            error_code: 0,
            name: "topic-v8",
            is_internal: false,
            partitions: [
              %{
                error_code: 0,
                partition_index: 0,
                leader_id: 1,
                leader_epoch: 5,
                replica_nodes: [1],
                isr_nodes: [1],
                offline_replicas: []
              }
            ],
            topic_authorized_operations: 2_048
          }
        ],
        cluster_authorized_operations: 4_096
      }

      {:ok, cluster_metadata} = MetadataResponse.parse_response(response)

      assert cluster_metadata.controller_id == 1
      topic = cluster_metadata.topics["topic-v8"]
      assert topic.name == "topic-v8"
      # authorized_operations not in domain structs, but parsing should not fail
    end

    test "V8 response struct adds cluster_authorized_operations vs V7" do
      v7 = %V7.Response{}
      v8 = %V8.Response{}

      v7_keys = Map.keys(v7) -- [:__struct__]
      v8_keys = Map.keys(v8) -- [:__struct__]

      new_fields = v8_keys -- v7_keys
      assert :cluster_authorized_operations in new_fields
    end
  end

  # ---------------------------------------------------------------------------
  # V9 - flexible version with compact encodings + tagged_fields
  # ---------------------------------------------------------------------------

  describe "parse_response/1 for V9 (flexible version)" do
    test "parses response with tagged_fields" do
      response = %V9.Response{
        throttle_time_ms: 0,
        brokers: [
          %{
            node_id: 1,
            host: "broker1.example.com",
            port: 9092,
            rack: "rack-1",
            tagged_fields: []
          }
        ],
        cluster_id: "cluster-v9",
        controller_id: 1,
        topics: [
          %{
            error_code: 0,
            name: "topic-v9",
            is_internal: false,
            partitions: [
              %{
                error_code: 0,
                partition_index: 0,
                leader_id: 1,
                leader_epoch: 99,
                replica_nodes: [1],
                isr_nodes: [1],
                offline_replicas: [],
                tagged_fields: []
              }
            ],
            topic_authorized_operations: -2_147_483_648,
            tagged_fields: []
          }
        ],
        cluster_authorized_operations: -2_147_483_648,
        tagged_fields: []
      }

      {:ok, cluster_metadata} = MetadataResponse.parse_response(response)

      assert cluster_metadata.controller_id == 1
      assert cluster_metadata.brokers[1].rack == "rack-1"
      topic = cluster_metadata.topics["topic-v9"]
      assert topic.name == "topic-v9"
      assert length(topic.partitions) == 1
    end

    test "parses response with unknown tagged_fields (silently ignored)" do
      response = %V9.Response{
        throttle_time_ms: 0,
        brokers: [
          %{
            node_id: 1,
            host: "broker1.example.com",
            port: 9092,
            rack: nil,
            tagged_fields: [{42, <<1, 2, 3>>}]
          }
        ],
        cluster_id: nil,
        controller_id: 1,
        topics: [],
        cluster_authorized_operations: 0,
        tagged_fields: [{99, <<4, 5, 6>>}]
      }

      {:ok, cluster_metadata} = MetadataResponse.parse_response(response)

      assert map_size(cluster_metadata.brokers) == 1
      assert cluster_metadata.brokers[1].host == "broker1.example.com"
    end

    test "V9 response struct adds tagged_fields vs V8" do
      v8 = %V8.Response{}
      v9 = %V9.Response{}

      v8_keys = Map.keys(v8) -- [:__struct__]
      v9_keys = Map.keys(v9) -- [:__struct__]

      new_fields = v9_keys -- v8_keys
      assert :tagged_fields in new_fields
    end
  end

  # ---------------------------------------------------------------------------
  # Edge cases (applicable to all versions)
  # ---------------------------------------------------------------------------

  describe "parse_response/1 edge cases" do
    test "handles empty brokers list (V0)" do
      response = %V0.Response{
        brokers: [],
        topics: []
      }

      {:ok, cluster_metadata} = MetadataResponse.parse_response(response)

      assert map_size(cluster_metadata.brokers) == 0
      assert map_size(cluster_metadata.topics) == 0
    end

    test "handles empty topics list (V1)" do
      response = %V1.Response{
        brokers: [
          %{node_id: 1, host: "broker1.example.com", port: 9092, rack: nil}
        ],
        controller_id: 1,
        topics: []
      }

      {:ok, cluster_metadata} = MetadataResponse.parse_response(response)

      assert map_size(cluster_metadata.topics) == 0
    end

    test "handles topic with no partitions (V1)" do
      response = %V1.Response{
        brokers: [
          %{node_id: 1, host: "broker1.example.com", port: 9092, rack: nil}
        ],
        controller_id: 1,
        topics: [
          %{
            error_code: 0,
            name: "empty-topic",
            is_internal: false,
            partitions: []
          }
        ]
      }

      {:ok, cluster_metadata} = MetadataResponse.parse_response(response)

      topic = cluster_metadata.topics["empty-topic"]
      assert topic.name == "empty-topic"
      assert length(topic.partitions) == 0
      assert topic.partition_leaders == %{}
    end

    test "handles all topics having errors (V3)" do
      response = %V3.Response{
        throttle_time_ms: 0,
        brokers: [base_broker()],
        cluster_id: nil,
        controller_id: 1,
        topics: [
          %{error_code: 3, name: "err1", is_internal: false, partitions: []},
          %{error_code: 5, name: "err2", is_internal: false, partitions: []}
        ]
      }

      {:ok, cluster_metadata} = MetadataResponse.parse_response(response)

      assert map_size(cluster_metadata.topics) == 0
    end

    test "handles many brokers and topics (V5)" do
      brokers =
        for n <- 1..10 do
          %{node_id: n, host: "broker#{n}.example.com", port: 9092, rack: "rack-#{rem(n, 3)}"}
        end

      topics =
        for t <- 1..5 do
          %{
            error_code: 0,
            name: "topic-#{t}",
            is_internal: false,
            partitions:
              for p <- 0..2 do
                %{
                  error_code: 0,
                  partition_index: p,
                  leader_id: rem(p + t, 10) + 1,
                  replica_nodes: [1, 2, 3],
                  isr_nodes: [1, 2],
                  offline_replicas: []
                }
              end
          }
        end

      response = %V5.Response{
        throttle_time_ms: 250,
        brokers: brokers,
        cluster_id: "large-cluster",
        controller_id: 1,
        topics: topics
      }

      {:ok, cluster_metadata} = MetadataResponse.parse_response(response)

      assert map_size(cluster_metadata.brokers) == 10
      assert map_size(cluster_metadata.topics) == 5

      for t <- 1..5 do
        topic = cluster_metadata.topics["topic-#{t}"]
        assert length(topic.partitions) == 3
      end
    end
  end

  # ---------------------------------------------------------------------------
  # parse_response via KayrockProtocol (dispatching)
  # ---------------------------------------------------------------------------

  describe "parse_response via KayrockProtocol" do
    alias KafkaEx.Protocol.KayrockProtocol

    test "dispatches V0 response" do
      response = %V0.Response{
        brokers: [%{node_id: 1, host: "h", port: 9092}],
        topics: []
      }

      {:ok, result} = KayrockProtocol.parse_response(:metadata, response)

      assert %ClusterMetadata{} = result
    end

    test "dispatches V1 response" do
      response = %V1.Response{
        brokers: [%{node_id: 1, host: "h", port: 9092, rack: nil}],
        controller_id: 1,
        topics: []
      }

      {:ok, result} = KayrockProtocol.parse_response(:metadata, response)

      assert %ClusterMetadata{} = result
      assert result.controller_id == 1
    end

    test "dispatches V3 response" do
      response = %V3.Response{
        throttle_time_ms: 100,
        brokers: [base_broker()],
        cluster_id: "c",
        controller_id: 1,
        topics: [base_topic()]
      }

      {:ok, result} = KayrockProtocol.parse_response(:metadata, response)

      assert %ClusterMetadata{} = result
      assert result.controller_id == 1
      assert map_size(result.topics) == 1
    end

    test "dispatches V9 response" do
      response = %V9.Response{
        throttle_time_ms: 0,
        brokers: [
          %{node_id: 1, host: "h", port: 9092, rack: nil, tagged_fields: []}
        ],
        cluster_id: nil,
        controller_id: 1,
        topics: [],
        cluster_authorized_operations: 0,
        tagged_fields: []
      }

      {:ok, result} = KayrockProtocol.parse_response(:metadata, response)

      assert %ClusterMetadata{} = result
    end
  end

  # ---------------------------------------------------------------------------
  # Consistency across versions
  # ---------------------------------------------------------------------------

  describe "consistency across versions" do
    test "V0 through V9 produce same ClusterMetadata for equivalent data" do
      brokers_plain = [%{node_id: 1, host: "b1", port: 9092}]

      brokers_with_rack = [%{node_id: 1, host: "b1", port: 9092, rack: "r1"}]

      brokers_with_tagged = [
        %{node_id: 1, host: "b1", port: 9092, rack: "r1", tagged_fields: []}
      ]

      topic_base = %{
        error_code: 0,
        name: "t",
        is_internal: false,
        partitions: [
          %{error_code: 0, partition_index: 0, leader_id: 1, replica_nodes: [1], isr_nodes: [1]}
        ]
      }

      topic_with_offline = put_in(topic_base, [:partitions, Access.at(0), :offline_replicas], [])
      topic_with_epoch = put_in(topic_with_offline, [:partitions, Access.at(0), :leader_epoch], 1)
      topic_with_ops = Map.put(topic_with_epoch, :topic_authorized_operations, 0)
      topic_with_tagged = Map.put(topic_with_ops, :tagged_fields, [])

      topic_with_partition_tagged =
        update_in(topic_with_tagged, [:partitions, Access.at(0)], &Map.put(&1, :tagged_fields, []))

      responses = [
        %V0.Response{brokers: brokers_plain, topics: [topic_base]},
        %V1.Response{
          brokers: brokers_with_rack,
          controller_id: 1,
          topics: [topic_base]
        },
        %V2.Response{
          brokers: brokers_with_rack,
          cluster_id: "c",
          controller_id: 1,
          topics: [topic_base]
        },
        %V3.Response{
          throttle_time_ms: 0,
          brokers: brokers_with_rack,
          cluster_id: "c",
          controller_id: 1,
          topics: [topic_base]
        },
        %V4.Response{
          throttle_time_ms: 0,
          brokers: brokers_with_rack,
          cluster_id: "c",
          controller_id: 1,
          topics: [topic_base]
        },
        %V5.Response{
          throttle_time_ms: 0,
          brokers: brokers_with_rack,
          cluster_id: "c",
          controller_id: 1,
          topics: [topic_with_offline]
        },
        %V6.Response{
          throttle_time_ms: 0,
          brokers: brokers_with_rack,
          cluster_id: "c",
          controller_id: 1,
          topics: [topic_with_offline]
        },
        %V7.Response{
          throttle_time_ms: 0,
          brokers: brokers_with_rack,
          cluster_id: "c",
          controller_id: 1,
          topics: [topic_with_epoch]
        },
        %V8.Response{
          throttle_time_ms: 0,
          brokers: brokers_with_rack,
          cluster_id: "c",
          controller_id: 1,
          topics: [topic_with_ops],
          cluster_authorized_operations: 0
        },
        %V9.Response{
          throttle_time_ms: 0,
          brokers: brokers_with_tagged,
          cluster_id: "c",
          controller_id: 1,
          topics: [topic_with_partition_tagged],
          cluster_authorized_operations: 0,
          tagged_fields: []
        }
      ]

      results =
        Enum.map(responses, fn resp ->
          {:ok, cm} = MetadataResponse.parse_response(resp)
          cm
        end)

      # All should produce exactly one topic named "t" with one partition
      for cm <- results do
        assert map_size(cm.topics) == 1
        topic = cm.topics["t"]
        assert topic.name == "t"
        assert length(topic.partitions) == 1
        assert topic.partition_leaders == %{0 => 1}
      end

      # V1+ should all have controller_id == 1
      [_v0 | rest] = results

      for cm <- rest do
        assert cm.controller_id == 1
      end

      # V1+ should all have rack "r1" on broker 1
      for cm <- rest do
        assert cm.brokers[1].rack == "r1"
      end
    end
  end

  # ---------------------------------------------------------------------------
  # Any fallback (forward compatibility)
  # ---------------------------------------------------------------------------

  describe "Any fallback response implementation (forward compatibility)" do
    defmodule FakeV10Response do
      defstruct [
        :throttle_time_ms,
        :brokers,
        :cluster_id,
        :controller_id,
        :topics,
        :cluster_authorized_operations,
        :tagged_fields,
        :new_future_field
      ]
    end

    test "parses unknown struct via Any fallback" do
      response = %FakeV10Response{
        throttle_time_ms: 0,
        brokers: [%{node_id: 1, host: "h", port: 9092, rack: nil}],
        cluster_id: "future",
        controller_id: 1,
        topics: [
          %{
            error_code: 0,
            name: "t",
            is_internal: false,
            partitions: [
              %{error_code: 0, partition_index: 0, leader_id: 1, replica_nodes: [1], isr_nodes: [1]}
            ]
          }
        ],
        cluster_authorized_operations: 0,
        tagged_fields: [],
        new_future_field: "unknown"
      }

      {:ok, cluster_metadata} = MetadataResponse.parse_response(response)

      assert %ClusterMetadata{} = cluster_metadata
      assert cluster_metadata.controller_id == 1
      assert map_size(cluster_metadata.topics) == 1
    end
  end
end
