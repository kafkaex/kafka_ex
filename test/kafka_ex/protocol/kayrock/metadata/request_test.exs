defmodule KafkaEx.Protocol.Kayrock.Metadata.RequestTest do
  use ExUnit.Case, async: true

  alias KafkaEx.Protocol.Kayrock.Metadata.Request, as: MetadataRequest
  alias Kayrock.Metadata.V0
  alias Kayrock.Metadata.V1
  alias Kayrock.Metadata.V2

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
      request = MetadataRequest.build_request(%V0.Request{}, topics: ["topic1", "topic2"])

      assert request == %V0.Request{
               client_id: nil,
               correlation_id: nil,
               topics: ["topic1", "topic2"]
             }
    end

    test "builds request for single topic" do
      request = MetadataRequest.build_request(%V0.Request{}, topics: ["my-topic"])

      assert request == %V0.Request{
               client_id: nil,
               correlation_id: nil,
               topics: ["my-topic"]
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
               topics: ["topic1"]
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
               topics: ["topic1", "topic2", "topic3"]
             }
    end

    test "builds request for single topic" do
      request = MetadataRequest.build_request(%V1.Request{}, topics: ["single-topic"])

      assert request == %V1.Request{
               client_id: nil,
               correlation_id: nil,
               topics: ["single-topic"]
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
               topics: ["topic-a"]
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
               topics: ["foo", "bar", "baz"]
             }
    end

    test "builds request for single topic" do
      request = MetadataRequest.build_request(%V2.Request{}, topics: ["test-topic"])

      assert request == %V2.Request{
               client_id: nil,
               correlation_id: nil,
               topics: ["test-topic"]
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
               topics: ["v2-topic"]
             }
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
  end
end
