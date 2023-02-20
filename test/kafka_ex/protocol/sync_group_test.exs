defmodule KafkaEx.Protocol.SyncGroup.Test do
  use ExUnit.Case, async: true

  test "create_request creates a valid sync group request" do
    first_assignments = <<
      # Version
      0::16,
      # PartitionAssignment array size
      1::32,
      # Topic
      6::16,
      "topic1",
      # Partition array
      3::32,
      1::32,
      3::32,
      5::32,
      # UserData
      0::32
    >>

    second_assignments = <<
      # Version
      0::16,
      # PartitionAssignment array size
      1::32,
      6::16,
      "topic1",
      # Partition array
      3::32,
      2::32,
      4::32,
      6::32,
      # UserData
      0::32
    >>

    good_request = <<
      # Preamble
      14::16,
      0::16,
      42::32,
      9::16,
      "client_id"::binary,
      # GroupId
      5::16,
      "group"::binary,
      # Generation ID
      1::32,
      # MemberId
      10::16,
      "member_one"::binary,
      # GroupAssignment array size
      2::32,
      # First member ID
      10::16,
      "member_one"::binary,
      byte_size(first_assignments)::32,
      first_assignments::binary,
      # Second member ID
      10::16,
      "member_two"::binary,
      byte_size(second_assignments)::32,
      second_assignments::binary
    >>

    sync_request = %KafkaEx.Protocol.SyncGroup.Request{
      group_name: "group",
      member_id: "member_one",
      generation_id: 1,
      assignments: [
        {"member_one", [{"topic1", [1, 3, 5]}]},
        {"member_two", [{"topic1", [2, 4, 6]}]}
      ]
    }

    request =
      KafkaEx.Protocol.SyncGroup.create_request(42, "client_id", sync_request)

    assert request == good_request
  end

  test "parse success response correctly" do
    member_assignment =
      <<0::16, 1::32, 6::16, "topic1", 3::32, 1::32, 3::32, 5::32>>

    response = <<
      # CorrelationId
      42::32,
      # ErrorCode
      0::16,
      # MemberAssignment
      byte_size(member_assignment)::32,
      member_assignment::binary
    >>

    expected_response = %KafkaEx.Protocol.SyncGroup.Response{
      error_code: :no_error,
      assignments: [{"topic1", [5, 3, 1]}]
    }

    assert KafkaEx.Protocol.SyncGroup.parse_response(response) ==
             expected_response
  end

  test "parse empty assignments correctly" do
    response = <<
      # CorrelationId
      42::32,
      # ErrorCode
      0::16,
      # MemberAssignment (empty)
      0::32
    >>

    expected_response = %KafkaEx.Protocol.SyncGroup.Response{
      error_code: :no_error,
      assignments: []
    }

    assert KafkaEx.Protocol.SyncGroup.parse_response(response) ==
             expected_response
  end
end
