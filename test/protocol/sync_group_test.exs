defmodule KafkaEx.Protocol.SyncGroup.Test do
  use ExUnit.Case, async: true

  test "create_request creates a valid sync group request" do
    first_assignments = <<
         0 :: 16, # Version
         1 :: 32, # PartitionAssignment array size
         6 :: 16, "topic1", # Topic
         3 :: 32, 1 :: 32, 3 :: 32, 5 :: 32, # Partition array
         0 :: 32, # UserData
      >>
    second_assignments = <<
         0 :: 16, # Version
         1 :: 32, # PartitionAssignment array size
         6 :: 16, "topic1",
         3 :: 32, 2 :: 32, 4 :: 32, 6 :: 32, # Partition array
         0 :: 32, # UserData
      >>
    good_request = <<
        14 :: 16, 0 :: 16, 42 :: 32, 9 :: 16, "client_id" :: binary, # Preamble
         5 :: 16, "group" :: binary, # GroupId
         1 :: 32, # Generation ID
        10 :: 16, "member_one" :: binary, # MemberId
         2 :: 32, # GroupAssignment array size
        10 :: 16, "member_one" :: binary, # First member ID
        byte_size(first_assignments) :: 32, first_assignments :: binary,
        10 :: 16, "member_two" :: binary, # Second member ID
        byte_size(second_assignments) :: 32, second_assignments :: binary
      >>

    sync_request = %KafkaEx.Protocol.SyncGroup.Request{
      group_name: "group",
      member_id: "member_one",
      generation_id: 1,
      assignments: [{"member_one", [{"topic1", [1, 3, 5]}]}, {"member_two", [{"topic1", [2, 4, 6]}]}],
    }

    request = KafkaEx.Protocol.SyncGroup.create_request(42, "client_id", sync_request)
    assert request == good_request
  end

  test "parse success response correctly" do
    member_assignment = << 0 :: 16, 1 :: 32, 6 :: 16, "topic1", 3 :: 32, 1 :: 32, 3 :: 32, 5 :: 32  >>
    response = <<
        42 :: 32, # CorrelationId
        0 :: 16, # ErrorCode
        byte_size(member_assignment) :: 32, member_assignment :: binary # MemberAssignment
      >>
    expected_response = %KafkaEx.Protocol.SyncGroup.Response{error_code: :no_error,
      assignments: [{"topic1", [5, 3, 1]}]}
    assert KafkaEx.Protocol.SyncGroup.parse_response(response) == expected_response
  end

  test "parse empty assignments correctly" do
    response = <<
        42 :: 32, # CorrelationId
        0 :: 16, # ErrorCode
        0 :: 32, # MemberAssignment (empty)
      >>
    expected_response = %KafkaEx.Protocol.SyncGroup.Response{error_code: :no_error, assignments: []}
    assert KafkaEx.Protocol.SyncGroup.parse_response(response) == expected_response
  end
end
