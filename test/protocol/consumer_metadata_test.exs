defmodule KafkaEx.Protocol.ConsumerMetadata.Test do
  use ExUnit.Case, async: true

  test "create_request creates a valid consumer metadata request" do
    good_request = <<10 :: 16, 0 :: 16, 1 :: 32, 3 :: 16, "foo", 2 :: 16, "we" >>
    request = KafkaEx.Protocol.ConsumerMetadata.create_request(1, "foo", "we")
    assert request == good_request
  end

  test "parse_response correctly parses a valid response" do
    response = <<0, 0, 0, 1, 0, 0, 0, 0, 192, 2, 0, 14, 49, 57, 50, 46, 49, 54, 56, 46, 53, 57, 46, 49, 48, 51, 0, 0, 192, 2>>
    #parse response
  end
end
