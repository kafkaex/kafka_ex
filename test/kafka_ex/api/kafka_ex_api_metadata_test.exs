defmodule KafkaEx.APIMetadataTest do
  use ExUnit.Case, async: false

  alias KafkaEx.API, as: KafkaExAPI
  alias KafkaEx.Cluster.{Broker, ClusterMetadata, PartitionInfo, Topic}

  # Mock GenServer for testing
  defmodule MockClient do
    use GenServer

    def start_link(initial_state) do
      GenServer.start_link(__MODULE__, initial_state)
    end

    def init(state) do
      {:ok, state}
    end

    def handle_call(:cluster_metadata, _from, state) do
      {:reply, {:ok, state.cluster_metadata}, state}
    end

    def handle_call({:metadata, topics, opts, api_version}, _from, state) do
      # Store the call args for test verification
      call_info = %{topics: topics, opts: opts, api_version: api_version}
      new_state = Map.put(state, :last_call, call_info)

      # Return configured response or default
      response = Map.get(state, :metadata_response, {:ok, state.cluster_metadata})
      {:reply, response, new_state}
    end

    def handle_call(:get_last_call, _from, state) do
      {:reply, Map.get(state, :last_call), state}
    end
  end

  describe "KafkaExAPI.cluster_metadata/1" do
    test "returns cached cluster metadata without network request" do
      {:ok, client} = MockClient.start_link(%{cluster_metadata: build_empty_cluster_metadata()})

      {:ok, metadata} = KafkaExAPI.cluster_metadata(client)

      assert %ClusterMetadata{} = metadata
      assert metadata.brokers == %{}
      assert metadata.topics == %{}
    end

    test "returns cluster metadata with brokers and topics" do
      {:ok, client} = MockClient.start_link(%{cluster_metadata: build_cluster_metadata_with_data()})

      {:ok, metadata} = KafkaExAPI.cluster_metadata(client)

      assert map_size(metadata.brokers) == 2
      assert map_size(metadata.topics) == 1
      assert metadata.controller_id == 1
    end
  end

  describe "KafkaExAPI.metadata/1" do
    test "fetches metadata for all topics with default options" do
      {:ok, client} =
        MockClient.start_link(%{
          cluster_metadata: build_empty_cluster_metadata(),
          metadata_response: {:ok, build_empty_cluster_metadata()}
        })

      {:ok, metadata} = KafkaExAPI.metadata(client)

      assert %ClusterMetadata{} = metadata

      # Verify the call was made with correct arguments
      last_call = GenServer.call(client, :get_last_call)
      assert last_call.topics == nil
      assert last_call.api_version == 1
    end

    test "fetches metadata with custom API version" do
      {:ok, client} =
        MockClient.start_link(%{
          cluster_metadata: build_empty_cluster_metadata(),
          metadata_response: {:ok, build_empty_cluster_metadata()}
        })

      {:ok, _metadata} = KafkaExAPI.metadata(client, api_version: 2)

      last_call = GenServer.call(client, :get_last_call)
      assert last_call.api_version == 2
      assert Keyword.get(last_call.opts, :api_version) == 2
    end

    test "passes through timeout option" do
      {:ok, client} =
        MockClient.start_link(%{
          cluster_metadata: build_empty_cluster_metadata(),
          metadata_response: {:ok, build_empty_cluster_metadata()}
        })

      {:ok, _metadata} = KafkaExAPI.metadata(client, timeout: 10_000)

      last_call = GenServer.call(client, :get_last_call)
      assert Keyword.get(last_call.opts, :timeout) == 10_000
    end

    test "returns error when request fails" do
      {:ok, client} =
        MockClient.start_link(%{
          cluster_metadata: build_empty_cluster_metadata(),
          metadata_response: {:error, :timeout}
        })

      assert {:error, :timeout} = KafkaExAPI.metadata(client)
    end
  end

  describe "KafkaExAPI.metadata/3" do
    test "fetches metadata for specific topics" do
      topics = ["orders", "payments"]

      {:ok, client} =
        MockClient.start_link(%{
          cluster_metadata: build_empty_cluster_metadata(),
          metadata_response: {:ok, build_cluster_metadata_with_topics(topics)}
        })

      {:ok, metadata} = KafkaExAPI.metadata(client, topics, [])

      assert %ClusterMetadata{} = metadata
      assert Map.has_key?(metadata.topics, "orders")
      assert Map.has_key?(metadata.topics, "payments")

      last_call = GenServer.call(client, :get_last_call)
      assert last_call.topics == topics
      assert Keyword.get(last_call.opts, :topics) == topics
    end

    test "accepts nil for all topics" do
      {:ok, client} =
        MockClient.start_link(%{
          cluster_metadata: build_empty_cluster_metadata(),
          metadata_response: {:ok, build_empty_cluster_metadata()}
        })

      {:ok, metadata} = KafkaExAPI.metadata(client, nil, [])

      assert %ClusterMetadata{} = metadata

      last_call = GenServer.call(client, :get_last_call)
      assert last_call.topics == nil
    end

    test "passes allow_auto_topic_creation option" do
      topics = ["new-topic"]

      {:ok, client} =
        MockClient.start_link(%{
          cluster_metadata: build_empty_cluster_metadata(),
          metadata_response: {:ok, build_cluster_metadata_with_topics(topics)}
        })

      {:ok, _metadata} = KafkaExAPI.metadata(client, topics, allow_auto_topic_creation: true)

      last_call = GenServer.call(client, :get_last_call)
      assert Keyword.get(last_call.opts, :allow_auto_topic_creation) == true
    end

    test "fetches metadata with custom API version" do
      topics = ["orders"]

      {:ok, client} =
        MockClient.start_link(%{
          cluster_metadata: build_empty_cluster_metadata(),
          metadata_response: {:ok, build_cluster_metadata_with_topics(topics)}
        })

      {:ok, _metadata} = KafkaExAPI.metadata(client, topics, api_version: 0)

      last_call = GenServer.call(client, :get_last_call)
      assert last_call.api_version == 0
    end

    test "returns error when request fails" do
      {:ok, client} =
        MockClient.start_link(%{
          cluster_metadata: build_empty_cluster_metadata(),
          metadata_response: {:error, :unknown_topic_or_partition}
        })

      assert {:error, :unknown_topic_or_partition} =
               KafkaExAPI.metadata(client, ["bad-topic"], [])
    end
  end

  describe "KafkaExAPI.metadata/2 integration with ClusterMetadata" do
    test "returned metadata contains broker information" do
      {:ok, client} =
        MockClient.start_link(%{
          cluster_metadata: build_empty_cluster_metadata(),
          metadata_response: {:ok, build_cluster_metadata_with_brokers()}
        })

      {:ok, metadata} = KafkaExAPI.metadata(client)

      assert map_size(metadata.brokers) == 3
      assert metadata.brokers[1].host == "broker1.local"
      assert metadata.brokers[2].host == "broker2.local"
      assert metadata.brokers[3].host == "broker3.local"
      assert metadata.controller_id == 2
    end

    test "returned metadata contains topic partition leaders" do
      {:ok, client} =
        MockClient.start_link(%{
          cluster_metadata: build_empty_cluster_metadata(),
          metadata_response: {:ok, build_cluster_metadata_with_partition_info()}
        })

      {:ok, metadata} = KafkaExAPI.metadata(client, ["orders"], [])

      topic = metadata.topics["orders"]
      assert topic.name == "orders"
      assert topic.partition_leaders == %{0 => 1, 1 => 2, 2 => 3}
      assert length(topic.partitions) == 3
    end

    test "returned metadata handles internal topics" do
      {:ok, client} =
        MockClient.start_link(%{
          cluster_metadata: build_empty_cluster_metadata(),
          metadata_response: {:ok, build_cluster_metadata_with_internal_topic()}
        })

      {:ok, metadata} = KafkaExAPI.metadata(client)

      internal_topic = metadata.topics["__consumer_offsets"]
      assert internal_topic.is_internal == true

      regular_topic = metadata.topics["orders"]
      assert regular_topic.is_internal == false
    end
  end

  describe "KafkaExAPI.metadata/3 edge cases" do
    test "handles empty topics list" do
      {:ok, client} =
        MockClient.start_link(%{
          cluster_metadata: build_empty_cluster_metadata(),
          metadata_response: {:ok, build_empty_cluster_metadata()}
        })

      {:ok, metadata} = KafkaExAPI.metadata(client, [], [])

      assert metadata.topics == %{}
    end

    test "handles single topic" do
      {:ok, client} =
        MockClient.start_link(%{
          cluster_metadata: build_empty_cluster_metadata(),
          metadata_response: {:ok, build_cluster_metadata_with_topics(["single"])}
        })

      {:ok, metadata} = KafkaExAPI.metadata(client, ["single"], [])

      assert map_size(metadata.topics) == 1
    end

    test "handles multiple options together" do
      {:ok, client} =
        MockClient.start_link(%{
          cluster_metadata: build_empty_cluster_metadata(),
          metadata_response: {:ok, build_cluster_metadata_with_topics(["topic1"])}
        })

      {:ok, metadata} =
        KafkaExAPI.metadata(
          client,
          ["topic1"],
          api_version: 2,
          timeout: 5000,
          allow_auto_topic_creation: true
        )

      assert %ClusterMetadata{} = metadata

      last_call = GenServer.call(client, :get_last_call)
      assert last_call.api_version == 2
      assert Keyword.get(last_call.opts, :timeout) == 5000
      assert Keyword.get(last_call.opts, :allow_auto_topic_creation) == true
    end
  end

  # Helper functions

  defp build_empty_cluster_metadata do
    %ClusterMetadata{
      brokers: %{},
      controller_id: nil,
      topics: %{},
      consumer_group_coordinators: %{}
    }
  end

  defp build_cluster_metadata_with_data do
    %ClusterMetadata{
      brokers: %{
        1 => %Broker{node_id: 1, host: "broker1", port: 9092, socket: nil, rack: nil},
        2 => %Broker{node_id: 2, host: "broker2", port: 9092, socket: nil, rack: nil}
      },
      controller_id: 1,
      topics: %{
        "test-topic" => %Topic{
          name: "test-topic",
          partition_leaders: %{0 => 1},
          is_internal: false,
          partitions: [%PartitionInfo{partition_id: 0, leader: 1, replicas: [1], isr: [1]}]
        }
      },
      consumer_group_coordinators: %{}
    }
  end

  defp build_cluster_metadata_with_topics(topics) do
    topic_map =
      Enum.into(topics, %{}, fn topic_name ->
        {topic_name,
         %Topic{
           name: topic_name,
           partition_leaders: %{0 => 1},
           is_internal: false,
           partitions: [%PartitionInfo{partition_id: 0, leader: 1, replicas: [1], isr: [1]}]
         }}
      end)

    %ClusterMetadata{
      brokers: %{1 => %Broker{node_id: 1, host: "broker1", port: 9092, socket: nil, rack: nil}},
      controller_id: 1,
      topics: topic_map,
      consumer_group_coordinators: %{}
    }
  end

  defp build_cluster_metadata_with_brokers do
    %ClusterMetadata{
      brokers: %{
        1 => %Broker{node_id: 1, host: "broker1.local", port: 9092, socket: nil, rack: "rack-1"},
        2 => %Broker{node_id: 2, host: "broker2.local", port: 9092, socket: nil, rack: "rack-2"},
        3 => %Broker{node_id: 3, host: "broker3.local", port: 9092, socket: nil, rack: "rack-1"}
      },
      controller_id: 2,
      topics: %{},
      consumer_group_coordinators: %{}
    }
  end

  defp build_cluster_metadata_with_partition_info do
    %ClusterMetadata{
      brokers: %{
        1 => %Broker{node_id: 1, host: "broker1", port: 9092, socket: nil, rack: nil},
        2 => %Broker{node_id: 2, host: "broker2", port: 9092, socket: nil, rack: nil},
        3 => %Broker{node_id: 3, host: "broker3", port: 9092, socket: nil, rack: nil}
      },
      controller_id: 1,
      topics: %{
        "orders" => %Topic{
          name: "orders",
          partition_leaders: %{0 => 1, 1 => 2, 2 => 3},
          is_internal: false,
          partitions: [
            %PartitionInfo{partition_id: 0, leader: 1, replicas: [1, 2], isr: [1, 2]},
            %PartitionInfo{partition_id: 1, leader: 2, replicas: [2, 3], isr: [2, 3]},
            %PartitionInfo{partition_id: 2, leader: 3, replicas: [3, 1], isr: [3, 1]}
          ]
        }
      },
      consumer_group_coordinators: %{}
    }
  end

  defp build_cluster_metadata_with_internal_topic do
    %ClusterMetadata{
      brokers: %{
        1 => %Broker{node_id: 1, host: "broker1", port: 9092, socket: nil, rack: nil}
      },
      controller_id: 1,
      topics: %{
        "__consumer_offsets" => %Topic{
          name: "__consumer_offsets",
          partition_leaders: %{0 => 1},
          is_internal: true,
          partitions: [%PartitionInfo{partition_id: 0, leader: 1, replicas: [1], isr: [1]}]
        },
        "orders" => %Topic{
          name: "orders",
          partition_leaders: %{0 => 1},
          is_internal: false,
          partitions: [%PartitionInfo{partition_id: 0, leader: 1, replicas: [1], isr: [1]}]
        }
      },
      consumer_group_coordinators: %{}
    }
  end
end
