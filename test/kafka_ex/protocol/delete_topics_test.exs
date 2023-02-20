defmodule KafkaEx.Protocol.DeleteTopicsTest do
  use ExUnit.Case, async: true

  describe "create_request/4" do
    test "creates a request to delete a single topic" do
      expected_request =
        <<20::16, 0::16, 999::32, 13::16, "the-client-id"::binary, 1::32-signed,
          6::16, "topic1"::binary, 100::32-signed>>

      delete_request = %KafkaEx.Protocol.DeleteTopics.Request{
        topics: ["topic1"],
        timeout: 100
      }

      delete_response =
        KafkaEx.Protocol.DeleteTopics.create_request(
          999,
          "the-client-id",
          delete_request,
          0
        )

      assert expected_request == delete_response
    end

    test "creates a request to delete a multiple topic" do
      expected_response =
        <<20::16, 0::16, 999::32, 13::16, "the-client-id"::binary, 3::32-signed,
          6::16, "topic3"::binary, 6::16, "topic2"::binary, 6::16,
          "topic1"::binary, 100::32-signed>>

      delete_request = %KafkaEx.Protocol.DeleteTopics.Request{
        topics: ["topic1", "topic2", "topic3"],
        timeout: 100
      }

      delete_response =
        KafkaEx.Protocol.DeleteTopics.create_request(
          999,
          "the-client-id",
          delete_request,
          0
        )

      assert expected_response == delete_response
    end

    test "raise error when non-zero api_version is sent" do
      delete_request = %KafkaEx.Protocol.DeleteTopics.Request{
        topics: ["topic1"],
        timeout: 100
      }

      assert_raise FunctionClauseError,
                   "no function clause matching in KafkaEx.Protocol.DeleteTopics.create_request/4",
                   fn ->
                     KafkaEx.Protocol.DeleteTopics.create_request(
                       999,
                       "the-client-id",
                       delete_request,
                       1
                     )
                   end
    end
  end
end
