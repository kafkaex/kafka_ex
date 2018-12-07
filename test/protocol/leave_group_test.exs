defmodule KafkaEx.Protocol.LeaveGroup.Test do
  use ExUnit.Case, async: true

  test "create_request creates a valid leave group request" do
    good_request = <<
      # Preamble
      13::16,
      0::16,
      42::32,
      9::16,
      "client_id"::binary,
      # GroupId
      byte_size("group")::16,
      "group"::binary,
      # MemberId
      byte_size("member")::16,
      "member"::binary
    >>

    leave_request = %KafkaEx.Protocol.LeaveGroup.Request{
      group_name: "group",
      member_id: "member"
    }

    request =
      KafkaEx.Protocol.LeaveGroup.create_request(42, "client_id", leave_request)

    assert request == good_request
  end

  test "parse_response parses successful response correctly" do
    response = <<
      # CorrelationId
      42::32,
      # ErrorCode
      0::16
    >>

    expected_response = %KafkaEx.Protocol.LeaveGroup.Response{
      error_code: :no_error
    }

    assert KafkaEx.Protocol.LeaveGroup.parse_response(response) ==
             expected_response
  end
end
