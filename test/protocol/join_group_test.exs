defmodule KafkaEx.Protocol.JoinGroup.Test do
  use ExUnit.Case, async: true
  alias KafkaEx.Protocol.JoinGroup

  test "create_request creates a valid join group request" do
    good_request = <<
      # Preamble
      11::16,
      0::16,
      42::32,
      9::16,
      "client_id"::binary,
      # GroupId
      5::16,
      "group"::binary,
      # SessionTimeout
      3600::32,
      # MemberId
      9::16,
      "member_id"::binary,
      # ProtocolType
      8::16,
      "consumer"::binary,
      # GroupProtocols array size
      1::32,
      # Basic strategy, "roundrobin" has some restrictions
      6::16,
      "assign"::binary,
      # length of metadata
      32::32,
      # v0
      0::16,
      # Topics array
      2::32,
      9::16,
      "topic_one"::binary,
      9::16,
      "topic_two"::binary,
      # UserData
      0::32
    >>

    request =
      JoinGroup.create_request(42, "client_id", %JoinGroup.Request{
        member_id: "member_id",
        group_name: "group",
        topics: ["topic_one", "topic_two"],
        session_timeout: 3600
      })

    assert request == good_request
  end

  test "parse success response correctly" do
    response = <<
      # CorrelationId
      42::32,
      # ErrorCode
      0::16,
      # GenerationId
      123::32,
      # GroupProtocol
      8::16,
      "consumer"::binary,
      # LeaderId
      10::16,
      "member_xxx"::binary,
      # MemberId
      10::16,
      "member_one"::binary,
      # Members array
      2::32,
      10::16,
      "member_one",
      12::32,
      "metadata_one",
      10::16,
      "member_two",
      12::32,
      "metadata_two"
    >>

    expected_response = %JoinGroup.Response{
      error_code: :no_error,
      generation_id: 123,
      leader_id: "member_xxx",
      member_id: "member_one",
      members: ["member_two", "member_one"]
    }

    assert JoinGroup.parse_response(response) == expected_response
  end
end
