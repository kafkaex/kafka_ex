defmodule KafkaEx.Protocol.Heartbeat.Test do
  use ExUnit.Case, async: true

  test "create_request creates a valid heartbeat request" do
    good_request = <<
      12 :: 16, 0 :: 16, 42 :: 32, 9 :: 16, "client_id" :: binary, # Preamble
       8 :: 16, "group_id",
      1234 :: 32, # GenerationId
      9 :: 16, "member_id" :: binary # MemberId
    >>
    heartbeat_request = %KafkaEx.Protocol.Heartbeat.Request{
      group_name: "group_id",
      member_id: "member_id",
      generation_id: 1234,
    }

    request = KafkaEx.Protocol.Heartbeat.create_request(42, "client_id", heartbeat_request)
    assert request == good_request
  end

  test "parse success response correctly" do
    response = <<
      42 :: 32, # CorrelationId
       0 :: 16  # ErrorCode
    >>
    expected_response = %KafkaEx.Protocol.Heartbeat.Response{error_code: :no_error}
    assert KafkaEx.Protocol.Heartbeat.parse_response(response) == expected_response
  end
end
