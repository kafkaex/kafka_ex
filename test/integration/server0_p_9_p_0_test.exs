defmodule KafkaEx.Server0P9P0.Test do
  use ExUnit.Case
  import TestHelper

  alias KafkaEx.Protocol.Heartbeat.Request, as: HeartbeatRequest
  alias KafkaEx.Protocol.JoinGroup.Request, as: JoinGroupRequest
  alias KafkaEx.Protocol.LeaveGroup.Request, as: LeaveGroupRequest
  alias KafkaEx.Protocol.SyncGroup.Request, as: SyncGroupRequest

  @moduletag :server_0_p_9_p_0

  test "can join a consumer group" do
    random_group = generate_random_string()

    KafkaEx.create_worker(
      :join_group,
      uris: uris(),
      consumer_group: random_group
    )

    request = %JoinGroupRequest{
      group_name: random_group,
      member_id: "",
      topics: ["foo", "bar"],
      session_timeout: 6000
    }

    answer = KafkaEx.join_group(request, worker_name: :join_group)
    assert answer.error_code == :no_error
    assert answer.generation_id == 1
    # We should be the leader
    assert answer.member_id == answer.leader_id
  end

  test "can send a simple leader sync for a consumer group" do
    # A lot of repetition with the previous test. Leaving it in now, waiting for
    # how this pans out eventually as we add more and more 0.9 consumer group code
    random_group = generate_random_string()

    KafkaEx.create_worker(
      :sync_group,
      uris: uris(),
      consumer_group: random_group
    )

    request = %JoinGroupRequest{
      group_name: random_group,
      member_id: "",
      topics: ["foo", "bar"],
      session_timeout: 6000
    }

    answer = KafkaEx.join_group(request, worker_name: :sync_group)
    assert answer.error_code == :no_error

    member_id = answer.member_id
    generation_id = answer.generation_id
    my_assignments = [{"foo", [1]}, {"bar", [2]}]
    assignments = [{member_id, my_assignments}]

    request = %SyncGroupRequest{
      group_name: random_group,
      member_id: member_id,
      generation_id: generation_id,
      assignments: assignments
    }

    answer = KafkaEx.sync_group(request, worker_name: :sync_group)
    assert answer.error_code == :no_error

    # Parsing happens to return the assignments reversed, which is fine as there's no
    # ordering. Just reverse what we expect to match
    assert answer.assignments == Enum.reverse(my_assignments)
  end

  test "can leave a consumer group" do
    # A lot of repetition with the previous tests. Leaving it in now, waiting for
    # how this pans out eventually as we add more and more 0.9 consumer group code
    random_group = generate_random_string()

    KafkaEx.create_worker(
      :leave_group,
      uris: uris(),
      consumer_group: random_group
    )

    request = %JoinGroupRequest{
      group_name: random_group,
      member_id: "",
      topics: ["foo", "bar"],
      session_timeout: 6000
    }

    answer = KafkaEx.join_group(request, worker_name: :leave_group)
    assert answer.error_code == :no_error

    member_id = answer.member_id

    request = %LeaveGroupRequest{
      group_name: random_group,
      member_id: member_id
    }

    answer = KafkaEx.leave_group(request, worker_name: :leave_group)
    assert answer.error_code == :no_error
  end

  test "can heartbeat" do
    # See sync test. Removing repetition in the next iteration
    random_group = generate_random_string()

    KafkaEx.create_worker(
      :heartbeat,
      uris: uris(),
      consumer_group: random_group
    )

    request = %JoinGroupRequest{
      group_name: random_group,
      member_id: "",
      topics: ["foo", "bar"],
      session_timeout: 6000
    }

    answer = KafkaEx.join_group(request, worker_name: :heartbeat)
    assert answer.error_code == :no_error

    member_id = answer.member_id
    generation_id = answer.generation_id
    my_assignments = [{"foo", [1]}, {"bar", [2]}]
    assignments = [{member_id, my_assignments}]

    request = %SyncGroupRequest{
      group_name: random_group,
      member_id: member_id,
      generation_id: generation_id,
      assignments: assignments
    }

    answer = KafkaEx.sync_group(request, worker_name: :heartbeat)
    assert answer.error_code == :no_error

    request = %HeartbeatRequest{
      group_name: random_group,
      member_id: member_id,
      generation_id: generation_id
    }

    answer = KafkaEx.heartbeat(request, worker_name: :heartbeat)
    assert answer.error_code == :no_error
  end
end
