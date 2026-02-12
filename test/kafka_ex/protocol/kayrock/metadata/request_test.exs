defmodule KafkaEx.Protocol.Kayrock.Metadata.RequestTest do
  use ExUnit.Case, async: true

  alias KafkaEx.Protocol.Kayrock.Metadata.Request, as: MetadataRequest
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

  describe "build_request/2 for V0" do
    test "builds request for all topics (empty list)" do
      request = MetadataRequest.build_request(%V0.Request{}, topics: nil)

      assert request == %V0.Request{
               client_id: nil,
               correlation_id: nil,
               topics: []
             }
    end

    test "builds request for all topics (explicit empty list)" do
      request = MetadataRequest.build_request(%V0.Request{}, topics: [])

      assert request == %V0.Request{
               client_id: nil,
               correlation_id: nil,
               topics: []
             }
    end

    test "builds request for specific topics" do
      request = MetadataRequest.build_request(%V0.Request{}, topics: [%{name: "topic1"}, %{name: "topic2"}])

      assert request == %V0.Request{
               client_id: nil,
               correlation_id: nil,
               topics: [%{name: "topic1"}, %{name: "topic2"}]
             }
    end

    test "builds request for single topic" do
      request = MetadataRequest.build_request(%V0.Request{}, topics: ["my-topic"])

      assert request == %V0.Request{
               client_id: nil,
               correlation_id: nil,
               topics: [%{name: "my-topic"}]
             }
    end

    test "preserves existing correlation_id and client_id" do
      request =
        MetadataRequest.build_request(
          %V0.Request{correlation_id: 42, client_id: "test-client"},
          topics: ["topic1"]
        )

      assert request == %V0.Request{
               client_id: "test-client",
               correlation_id: 42,
               topics: [%{name: "topic1"}]
             }
    end
  end

  describe "build_request/2 for V1" do
    test "builds request for all topics (nil)" do
      request = MetadataRequest.build_request(%V1.Request{}, topics: nil)

      assert request == %V1.Request{
               client_id: nil,
               correlation_id: nil,
               topics: nil
             }
    end

    test "builds request for all topics (explicit empty list)" do
      request = MetadataRequest.build_request(%V1.Request{}, topics: [])

      assert request == %V1.Request{
               client_id: nil,
               correlation_id: nil,
               topics: nil
             }
    end

    test "builds request for specific topics" do
      request = MetadataRequest.build_request(%V1.Request{}, topics: ["topic1", "topic2", "topic3"])

      assert request == %V1.Request{
               client_id: nil,
               correlation_id: nil,
               topics: [%{name: "topic1"}, %{name: "topic2"}, %{name: "topic3"}]
             }
    end

    test "builds request for single topic" do
      request = MetadataRequest.build_request(%V1.Request{}, topics: ["single-topic"])

      assert request == %V1.Request{
               client_id: nil,
               correlation_id: nil,
               topics: [%{name: "single-topic"}]
             }
    end

    test "preserves existing correlation_id and client_id" do
      request =
        MetadataRequest.build_request(
          %V1.Request{correlation_id: 100, client_id: "my-client"},
          topics: ["topic-a"]
        )

      assert request == %V1.Request{
               client_id: "my-client",
               correlation_id: 100,
               topics: [%{name: "topic-a"}]
             }
    end
  end

  describe "build_request/2 for V2" do
    test "builds request for all topics (nil)" do
      request = MetadataRequest.build_request(%V2.Request{}, topics: nil)

      assert request == %V2.Request{
               client_id: nil,
               correlation_id: nil,
               topics: nil
             }
    end

    test "builds request for all topics (explicit empty list)" do
      request = MetadataRequest.build_request(%V2.Request{}, topics: [])

      assert request == %V2.Request{
               client_id: nil,
               correlation_id: nil,
               topics: nil
             }
    end

    test "builds request for specific topics" do
      request = MetadataRequest.build_request(%V2.Request{}, topics: ["foo", "bar", "baz"])

      assert request == %V2.Request{
               client_id: nil,
               correlation_id: nil,
               topics: [%{name: "foo"}, %{name: "bar"}, %{name: "baz"}]
             }
    end

    test "builds request for single topic" do
      request = MetadataRequest.build_request(%V2.Request{}, topics: ["test-topic"])

      assert request == %V2.Request{
               client_id: nil,
               correlation_id: nil,
               topics: [%{name: "test-topic"}]
             }
    end

    test "preserves existing correlation_id and client_id" do
      request =
        MetadataRequest.build_request(
          %V2.Request{correlation_id: 200, client_id: "v2-client"},
          topics: ["v2-topic"]
        )

      assert request == %V2.Request{
               client_id: "v2-client",
               correlation_id: 200,
               topics: [%{name: "v2-topic"}]
             }
    end
  end

  describe "build_request/2 for V3" do
    test "builds request for all topics (nil)" do
      request = MetadataRequest.build_request(%V3.Request{}, topics: nil)

      assert %V3.Request{} = request
      assert request.topics == nil
    end

    test "builds request for all topics (explicit empty list)" do
      request = MetadataRequest.build_request(%V3.Request{}, topics: [])

      assert %V3.Request{} = request
      assert request.topics == nil
    end

    test "builds request for specific topics" do
      request = MetadataRequest.build_request(%V3.Request{}, topics: ["t1", "t2"])

      assert %V3.Request{} = request
      assert request.topics == [%{name: "t1"}, %{name: "t2"}]
    end

    test "preserves correlation_id and client_id" do
      request =
        MetadataRequest.build_request(
          %V3.Request{correlation_id: 300, client_id: "v3-client"},
          topics: ["topic"]
        )

      assert request.correlation_id == 300
      assert request.client_id == "v3-client"
    end

    test "V3 request struct has same fields as V2 (no allow_auto_topic_creation)" do
      v2 = %V2.Request{}
      v3 = %V3.Request{}

      v2_keys = Map.keys(v2) |> MapSet.new()
      v3_keys = Map.keys(v3) |> MapSet.new()

      assert v2_keys == v3_keys
    end
  end

  describe "build_request/2 for V4" do
    test "builds request with default allow_auto_topic_creation" do
      request = MetadataRequest.build_request(%V4.Request{}, topics: ["topic1"])

      assert %V4.Request{} = request
      assert request.topics == [%{name: "topic1"}]
      assert request.allow_auto_topic_creation == false
    end

    test "builds request with allow_auto_topic_creation true" do
      request =
        MetadataRequest.build_request(%V4.Request{},
          topics: ["new-topic"],
          allow_auto_topic_creation: true
        )

      assert request.allow_auto_topic_creation == true
      assert request.topics == [%{name: "new-topic"}]
    end

    test "builds request with allow_auto_topic_creation false" do
      request =
        MetadataRequest.build_request(%V4.Request{},
          topics: ["topic"],
          allow_auto_topic_creation: false
        )

      assert request.allow_auto_topic_creation == false
    end

    test "builds request for all topics" do
      request = MetadataRequest.build_request(%V4.Request{}, topics: nil)

      assert request.topics == nil
      assert request.allow_auto_topic_creation == false
    end

    test "preserves correlation_id and client_id" do
      request =
        MetadataRequest.build_request(
          %V4.Request{correlation_id: 400, client_id: "v4-client"},
          topics: ["t"]
        )

      assert request.correlation_id == 400
      assert request.client_id == "v4-client"
    end

    test "V4 request struct adds allow_auto_topic_creation vs V3" do
      v3 = %V3.Request{}
      v4 = %V4.Request{}

      v3_keys = Map.keys(v3) -- [:__struct__]
      v4_keys = Map.keys(v4) -- [:__struct__]

      new_fields = v4_keys -- v3_keys
      assert :allow_auto_topic_creation in new_fields
    end
  end

  describe "build_request/2 for V5" do
    test "builds request with default options" do
      request = MetadataRequest.build_request(%V5.Request{}, topics: ["topic"])

      assert %V5.Request{} = request
      assert request.topics == [%{name: "topic"}]
      assert request.allow_auto_topic_creation == false
    end

    test "builds request with allow_auto_topic_creation" do
      request =
        MetadataRequest.build_request(%V5.Request{},
          topics: ["t"],
          allow_auto_topic_creation: true
        )

      assert request.allow_auto_topic_creation == true
    end

    test "V5 request struct has same fields as V4" do
      v4 = %V4.Request{}
      v5 = %V5.Request{}

      v4_keys = Map.keys(v4) |> MapSet.new()
      v5_keys = Map.keys(v5) |> MapSet.new()

      assert v4_keys == v5_keys
    end
  end

  describe "build_request/2 for V6" do
    test "builds request with default options" do
      request = MetadataRequest.build_request(%V6.Request{}, topics: ["topic"])

      assert %V6.Request{} = request
      assert request.topics == [%{name: "topic"}]
      assert request.allow_auto_topic_creation == false
    end

    test "builds request with allow_auto_topic_creation" do
      request =
        MetadataRequest.build_request(%V6.Request{},
          topics: ["t"],
          allow_auto_topic_creation: true
        )

      assert request.allow_auto_topic_creation == true
    end

    test "V6 request struct has same fields as V5" do
      v5 = %V5.Request{}
      v6 = %V6.Request{}

      v5_keys = Map.keys(v5) |> MapSet.new()
      v6_keys = Map.keys(v6) |> MapSet.new()

      assert v5_keys == v6_keys
    end
  end

  describe "build_request/2 for V7" do
    test "builds request with default options" do
      request = MetadataRequest.build_request(%V7.Request{}, topics: ["topic"])

      assert %V7.Request{} = request
      assert request.topics == [%{name: "topic"}]
      assert request.allow_auto_topic_creation == false
    end

    test "builds request with allow_auto_topic_creation" do
      request =
        MetadataRequest.build_request(%V7.Request{},
          topics: ["t"],
          allow_auto_topic_creation: true
        )

      assert request.allow_auto_topic_creation == true
    end

    test "V7 request struct has same fields as V6" do
      v6 = %V6.Request{}
      v7 = %V7.Request{}

      v6_keys = Map.keys(v6) |> MapSet.new()
      v7_keys = Map.keys(v7) |> MapSet.new()

      assert v6_keys == v7_keys
    end
  end

  describe "build_request/2 for V8" do
    test "builds request with default options" do
      request = MetadataRequest.build_request(%V8.Request{}, topics: ["topic"])

      assert %V8.Request{} = request
      assert request.topics == [%{name: "topic"}]
      assert request.allow_auto_topic_creation == false
      assert request.include_cluster_authorized_operations == false
      assert request.include_topic_authorized_operations == false
    end

    test "builds request with all options enabled" do
      request =
        MetadataRequest.build_request(%V8.Request{},
          topics: ["t"],
          allow_auto_topic_creation: true,
          include_cluster_authorized_operations: true,
          include_topic_authorized_operations: true
        )

      assert request.allow_auto_topic_creation == true
      assert request.include_cluster_authorized_operations == true
      assert request.include_topic_authorized_operations == true
    end

    test "builds request with only cluster authorized operations" do
      request =
        MetadataRequest.build_request(%V8.Request{},
          topics: ["t"],
          include_cluster_authorized_operations: true
        )

      assert request.include_cluster_authorized_operations == true
      assert request.include_topic_authorized_operations == false
    end

    test "builds request with only topic authorized operations" do
      request =
        MetadataRequest.build_request(%V8.Request{},
          topics: ["t"],
          include_topic_authorized_operations: true
        )

      assert request.include_cluster_authorized_operations == false
      assert request.include_topic_authorized_operations == true
    end

    test "builds request for all topics" do
      request = MetadataRequest.build_request(%V8.Request{}, topics: nil)

      assert request.topics == nil
      assert request.allow_auto_topic_creation == false
    end

    test "preserves correlation_id and client_id" do
      request =
        MetadataRequest.build_request(
          %V8.Request{correlation_id: 800, client_id: "v8-client"},
          topics: ["t"]
        )

      assert request.correlation_id == 800
      assert request.client_id == "v8-client"
    end

    test "V8 request struct adds authorized_operations fields vs V7" do
      v7 = %V7.Request{}
      v8 = %V8.Request{}

      v7_keys = Map.keys(v7) -- [:__struct__]
      v8_keys = Map.keys(v8) -- [:__struct__]

      new_fields = v8_keys -- v7_keys
      assert :include_cluster_authorized_operations in new_fields
      assert :include_topic_authorized_operations in new_fields
    end
  end

  describe "build_request/2 for V9 (flexible version)" do
    test "builds request with default options" do
      request = MetadataRequest.build_request(%V9.Request{}, topics: ["topic"])

      assert %V9.Request{} = request
      assert request.topics == [%{name: "topic"}]
      assert request.allow_auto_topic_creation == false
      assert request.include_cluster_authorized_operations == false
      assert request.include_topic_authorized_operations == false
    end

    test "builds request with all options enabled" do
      request =
        MetadataRequest.build_request(%V9.Request{},
          topics: ["t"],
          allow_auto_topic_creation: true,
          include_cluster_authorized_operations: true,
          include_topic_authorized_operations: true
        )

      assert request.allow_auto_topic_creation == true
      assert request.include_cluster_authorized_operations == true
      assert request.include_topic_authorized_operations == true
    end

    test "builds request for all topics (nil)" do
      request = MetadataRequest.build_request(%V9.Request{}, topics: nil)

      assert request.topics == nil
    end

    test "preserves tagged_fields" do
      request = %V9.Request{tagged_fields: [{0, <<1, 2, 3>>}]}

      result = MetadataRequest.build_request(request, topics: ["t"])

      assert result.tagged_fields == [{0, <<1, 2, 3>>}]
    end

    test "preserves correlation_id and client_id" do
      request =
        MetadataRequest.build_request(
          %V9.Request{correlation_id: 900, client_id: "v9-client"},
          topics: ["t"]
        )

      assert request.correlation_id == 900
      assert request.client_id == "v9-client"
    end

    test "V9 request struct adds tagged_fields vs V8" do
      v8 = %V8.Request{}
      v9 = %V9.Request{}

      v8_keys = Map.keys(v8) -- [:__struct__]
      v9_keys = Map.keys(v9) -- [:__struct__]

      new_fields = v9_keys -- v8_keys
      assert :tagged_fields in new_fields
    end
  end

  describe "build_request/2 with empty options" do
    test "V0 with empty options defaults to all topics" do
      request = MetadataRequest.build_request(%V0.Request{}, [])

      assert request.topics == []
    end

    test "V1 with empty options defaults to all topics" do
      request = MetadataRequest.build_request(%V1.Request{}, [])

      assert request.topics == nil
    end

    test "V2 with empty options defaults to all topics" do
      request = MetadataRequest.build_request(%V2.Request{}, [])

      assert request.topics == nil
    end

    test "V3 with empty options defaults to all topics" do
      request = MetadataRequest.build_request(%V3.Request{}, [])

      assert request.topics == nil
    end

    test "V4 with empty options defaults to all topics and auto-create off" do
      request = MetadataRequest.build_request(%V4.Request{}, [])

      assert request.topics == nil
      assert request.allow_auto_topic_creation == false
    end

    test "V5 with empty options defaults to all topics and auto-create off" do
      request = MetadataRequest.build_request(%V5.Request{}, [])

      assert request.topics == nil
      assert request.allow_auto_topic_creation == false
    end

    test "V6 with empty options defaults to all topics and auto-create off" do
      request = MetadataRequest.build_request(%V6.Request{}, [])

      assert request.topics == nil
      assert request.allow_auto_topic_creation == false
    end

    test "V7 with empty options defaults to all topics and auto-create off" do
      request = MetadataRequest.build_request(%V7.Request{}, [])

      assert request.topics == nil
      assert request.allow_auto_topic_creation == false
    end

    test "V8 with empty options defaults to all topics and all flags off" do
      request = MetadataRequest.build_request(%V8.Request{}, [])

      assert request.topics == nil
      assert request.allow_auto_topic_creation == false
      assert request.include_cluster_authorized_operations == false
      assert request.include_topic_authorized_operations == false
    end

    test "V9 with empty options defaults to all topics and all flags off" do
      request = MetadataRequest.build_request(%V9.Request{}, [])

      assert request.topics == nil
      assert request.allow_auto_topic_creation == false
      assert request.include_cluster_authorized_operations == false
      assert request.include_topic_authorized_operations == false
    end
  end

  describe "build_request via KayrockProtocol" do
    alias KafkaEx.Protocol.KayrockProtocol

    for version <- 0..9 do
      test "build_request dispatches to V#{version}" do
        result = KayrockProtocol.build_request(:metadata, unquote(version), topics: ["test"])

        expected_module = Module.concat([Kayrock.Metadata, :"V#{unquote(version)}", Request])
        assert result.__struct__ == expected_module
        assert result.topics == [%{name: "test"}]
      end
    end

    test "V4+ build_request sets allow_auto_topic_creation" do
      result =
        KayrockProtocol.build_request(:metadata, 4,
          topics: ["t"],
          allow_auto_topic_creation: true
        )

      assert result.allow_auto_topic_creation == true
    end

    test "V8+ build_request sets authorized_operations flags" do
      result =
        KayrockProtocol.build_request(:metadata, 8,
          topics: ["t"],
          include_cluster_authorized_operations: true,
          include_topic_authorized_operations: true
        )

      assert result.include_cluster_authorized_operations == true
      assert result.include_topic_authorized_operations == true
    end
  end

  describe "Any fallback request implementation (forward compatibility)" do
    defmodule FakeV10Request do
      defstruct [
        :correlation_id,
        :client_id,
        :topics,
        :allow_auto_topic_creation,
        :include_cluster_authorized_operations,
        :include_topic_authorized_operations,
        :new_future_field
      ]
    end

    test "handles unknown struct with all known fields via Any fallback" do
      request = %FakeV10Request{
        correlation_id: 10,
        client_id: "test"
      }

      result =
        MetadataRequest.build_request(request,
          topics: ["t"],
          allow_auto_topic_creation: true,
          include_cluster_authorized_operations: true,
          include_topic_authorized_operations: true
        )

      assert result.topics == [%{name: "t"}]
      assert result.allow_auto_topic_creation == true
      assert result.include_cluster_authorized_operations == true
      assert result.include_topic_authorized_operations == true
      assert result.correlation_id == 10
    end

    defmodule FakeMinimalRequest do
      defstruct [:correlation_id, :client_id, :topics]
    end

    test "handles unknown struct without optional fields via Any fallback" do
      request = %FakeMinimalRequest{correlation_id: 1, client_id: "c"}

      result = MetadataRequest.build_request(request, topics: ["t"])

      assert result.topics == [%{name: "t"}]
      assert result.correlation_id == 1
    end
  end
end
