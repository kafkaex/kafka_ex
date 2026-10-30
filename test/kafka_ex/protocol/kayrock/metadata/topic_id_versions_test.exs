defmodule KafkaEx.Protocol.Kayrock.Metadata.TopicIdVersionsTest do
  @moduledoc "Metadata V10-V13: topic ids, V11 field removal, V12 id lookups, V13 top-level error."
  use ExUnit.Case, async: true

  alias KafkaEx.Client.Error
  alias KafkaEx.Cluster.ClusterMetadata
  alias KafkaEx.Cluster.Topic
  alias KafkaEx.Protocol.Kayrock.Metadata.Request, as: MetadataRequest
  alias KafkaEx.Protocol.Kayrock.Metadata.RequestHelpers
  alias KafkaEx.Protocol.Kayrock.Metadata.Response, as: MetadataResponse
  alias KafkaEx.Protocol.Kayrock.Metadata.ResponseHelpers
  alias KafkaEx.Protocol.KayrockProtocol
  alias Kayrock.Metadata.V10
  alias Kayrock.Metadata.V11
  alias Kayrock.Metadata.V12
  alias Kayrock.Metadata.V13

  @id_a <<1::128>>
  @id_b <<0xDEADBEEF::128>>

  defp serialize!(request) do
    request
    |> Map.merge(%{correlation_id: 1, client_id: "test"})
    |> Kayrock.Request.serialize()
    |> IO.iodata_to_binary()
  end

  defp topic(name, id, error_code \\ 0) do
    %{
      error_code: error_code,
      name: name,
      topic_id: id,
      is_internal: false,
      partitions: [
        %{
          error_code: 0,
          partition_index: 0,
          leader_id: 1,
          leader_epoch: 3,
          replica_nodes: [1],
          isr_nodes: [1],
          offline_replicas: [],
          tagged_fields: []
        }
      ],
      topic_authorized_operations: -2_147_483_648,
      tagged_fields: []
    }
  end

  defp brokers, do: [%{node_id: 1, host: "b1", port: 9092, rack: nil, tagged_fields: []}]

  describe "V10 request" do
    test "sends topics by name with the null topic id" do
      request = MetadataRequest.build_request(%V10.Request{}, topics: ["a", "b"], allow_auto_topic_creation: true)

      assert %V10.Request{
               topics: [%{name: "a", topic_id: nil}, %{name: "b", topic_id: nil}],
               allow_auto_topic_creation: true,
               include_cluster_authorized_operations: false,
               include_topic_authorized_operations: false
             } = request

      assert is_binary(serialize!(request))
    end

    test "nil / [] topics request all topics" do
      assert %V10.Request{topics: nil} = MetadataRequest.build_request(%V10.Request{}, topics: nil)
      assert %V10.Request{topics: nil} = MetadataRequest.build_request(%V10.Request{}, topics: [])
      assert is_binary(serialize!(MetadataRequest.build_request(%V10.Request{}, [])))
    end

    test "ignores :topic_ids (V10 must not look topics up by id)" do
      request = MetadataRequest.build_request(%V10.Request{}, topics: ["a"], topic_ids: [@id_a])
      assert request.topics == [%{name: "a", topic_id: nil}]
    end
  end

  describe "V11 request" do
    test "has no include_cluster_authorized_operations field" do
      request =
        MetadataRequest.build_request(%V11.Request{},
          topics: ["a"],
          include_cluster_authorized_operations: true,
          include_topic_authorized_operations: true
        )

      refute Map.has_key?(request, :include_cluster_authorized_operations)
      assert request.include_topic_authorized_operations == true
      assert request.topics == [%{name: "a", topic_id: nil}]
      assert request.allow_auto_topic_creation == false
      assert is_binary(serialize!(request))
    end
  end

  describe "V12/V13 request" do
    for mod <- [V12, V13] do
      test "#{inspect(mod)} combines named topics and topic-id lookups" do
        request =
          MetadataRequest.build_request(struct(unquote(mod).Request), topics: ["a"], topic_ids: [@id_a, @id_b])

        assert request.topics == [
                 %{name: "a", topic_id: nil},
                 %{topic_id: @id_a, name: nil},
                 %{topic_id: @id_b, name: nil}
               ]

        assert is_binary(serialize!(request))
      end

      test "#{inspect(mod)} with only topic ids" do
        request = MetadataRequest.build_request(struct(unquote(mod).Request), topic_ids: [@id_a])
        assert request.topics == [%{topic_id: @id_a, name: nil}]
        assert is_binary(serialize!(request))
      end

      test "#{inspect(mod)} with neither requests all topics" do
        request = MetadataRequest.build_request(struct(unquote(mod).Request), topics: nil, topic_ids: [])
        assert request.topics == nil
        assert is_binary(serialize!(request))
      end
    end

    test "rejects a malformed topic id" do
      assert_raise FunctionClauseError, fn -> RequestHelpers.build_topic_id_list(topic_ids: ["not-a-uuid"]) end
    end
  end

  describe "V10-V12 response" do
    for mod <- [V10, V11, V12] do
      test "#{inspect(mod)} stores topic ids on Topic" do
        response =
          struct(unquote(mod).Response, %{
            throttle_time_ms: 0,
            brokers: brokers(),
            cluster_id: "c",
            controller_id: 1,
            topics: [topic("a", @id_a), topic("internal", <<0::128>>)],
            tagged_fields: []
          })

        assert {:ok, %ClusterMetadata{topics: topics} = metadata} = MetadataResponse.parse_response(response)
        assert %Topic{name: "a", topic_id: @id_a, partition_leaders: %{0 => 1}} = topics["a"]
        assert %Topic{topic_id: nil} = topics["internal"]
        assert ClusterMetadata.topic_name_by_id(metadata, @id_a) == {:ok, "a"}
      end
    end

    test "V12 reports a topic looked up by unknown id (null name + UNKNOWN_TOPIC_ID) instead of dropping it" do
      response = %V12.Response{
        throttle_time_ms: 0,
        brokers: brokers(),
        cluster_id: "c",
        controller_id: 1,
        topics: [topic("a", @id_a), topic(nil, @id_b, 100)],
        tagged_fields: []
      }

      # A miss must not look like success ({:ok, %{topics: %{}}}).
      assert {:error, %Error{error: :unknown_topic_id, metadata: %{topic_ids: [@id_b]}}} =
               MetadataResponse.parse_response(response)
    end

    test "V12 still drops an errored topic requested by name (unchanged behaviour)" do
      response = %V12.Response{
        throttle_time_ms: 0,
        brokers: brokers(),
        cluster_id: "c",
        controller_id: 1,
        # UNKNOWN_TOPIC_OR_PARTITION (3) for a by-name request carries the name
        topics: [topic("a", @id_a), topic("gone", <<0::128>>, 3)],
        tagged_fields: []
      }

      assert {:ok, %ClusterMetadata{topics: topics}} = MetadataResponse.parse_response(response)
      assert Map.keys(topics) == ["a"]
    end
  end

  describe "V13 response" do
    test "parses like V12 when error_code is 0" do
      response = %V13.Response{
        throttle_time_ms: 0,
        brokers: brokers(),
        cluster_id: "c",
        controller_id: 1,
        topics: [topic("a", @id_a)],
        error_code: 0,
        tagged_fields: []
      }

      assert {:ok, %ClusterMetadata{topics: %{"a" => %Topic{topic_id: @id_a}}}} =
               MetadataResponse.parse_response(response)
    end

    test "reports unknown topic ids like V12" do
      response = %V13.Response{
        throttle_time_ms: 0,
        brokers: brokers(),
        cluster_id: "c",
        controller_id: 1,
        topics: [topic(nil, @id_a, 100), topic(nil, @id_b, 100)],
        error_code: 0,
        tagged_fields: []
      }

      assert {:error, %Error{error: :unknown_topic_id, metadata: %{topic_ids: [@id_a, @id_b]}}} =
               MetadataResponse.parse_response(response)
    end

    test "surfaces a top-level error" do
      response = %V13.Response{
        throttle_time_ms: 0,
        brokers: [],
        cluster_id: nil,
        controller_id: -1,
        topics: [],
        error_code: 129,
        tagged_fields: []
      }

      assert {:error, %Error{error: :rebootstrap_required}} = MetadataResponse.parse_response(response)
    end
  end

  describe "via KayrockProtocol" do
    for version <- 10..13 do
      test "dispatches V#{version}" do
        request = KayrockProtocol.build_request(:metadata, unquote(version), topics: ["t"])
        assert request.__struct__ == Module.concat([Kayrock.Metadata, :"V#{unquote(version)}", Request])
        assert request.topics == [%{name: "t", topic_id: nil}]
      end
    end
  end

  describe "ResponseHelpers.normalize_topic_id/1" do
    test "maps the zero uuid and non-uuids to nil" do
      assert ResponseHelpers.normalize_topic_id(<<0::128>>) == nil
      assert ResponseHelpers.normalize_topic_id(nil) == nil
      assert ResponseHelpers.normalize_topic_id(@id_a) == @id_a
    end
  end

  describe "pre-V10 responses" do
    test "leave topic_id nil" do
      response = %Kayrock.Metadata.V9.Response{
        throttle_time_ms: 0,
        brokers: brokers(),
        cluster_id: "c",
        controller_id: 1,
        topics: [topic("a", nil) |> Map.delete(:topic_id)],
        cluster_authorized_operations: 0,
        tagged_fields: []
      }

      assert {:ok, %ClusterMetadata{topics: %{"a" => %Topic{topic_id: nil}}}} =
               MetadataResponse.parse_response(response)
    end
  end
end
