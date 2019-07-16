defmodule KafkaEx.KayrockCompatibility0p9p0Test do
  @moduledoc """
  These are tests using the original KafkaEx API with the kayrock server

  These come from server0_p_8_p_0_test.exs.  Note that even though Kayrock does
  not support this version of Kafka, the original implementations often delegate
  to previous versions. So unless the test is testing functionality that doesn't
  make sense in newer versions of kafka, we will test it here.
  """

  use ExUnit.Case

  @moduletag :server_kayrock

  alias KafkaEx.Protocol.Heartbeat.Request, as: HeartbeatRequest
  alias KafkaEx.Protocol.JoinGroup.Request, as: JoinGroupRequest
  alias KafkaEx.Protocol.LeaveGroup.Request, as: LeaveGroupRequest
  alias KafkaEx.Protocol.SyncGroup.Request, as: SyncGroupRequest

  alias KafkaEx.ServerKayrock

  setup do
    {:ok, args} = KafkaEx.build_worker_options([])

    {:ok, pid} = ServerKayrock.start_link(args, :no_name)

    {:ok, %{client: pid}}
  end

  test "can join a consumer group", %{client: client} do
    random_group = TestHelper.generate_random_string()

    request = %JoinGroupRequest{
      group_name: random_group,
      member_id: "",
      topics: ["foo", "bar"],
      session_timeout: 6000
    }

    answer = KafkaEx.join_group(request, worker_name: client, timeout: 10000)

    assert answer.error_code == :no_error
    assert answer.generation_id == 1
    # We should be the leader
    assert answer.member_id == answer.leader_id
  end

  test "can send a simple leader sync for a consumer group", %{client: client} do
    # A lot of repetition with the previous test. Leaving it in now, waiting for
    # how this pans out eventually as we add more and more 0.9 consumer group code
    random_group = TestHelper.generate_random_string()

    request = %JoinGroupRequest{
      group_name: random_group,
      member_id: "",
      topics: ["foo", "bar"],
      session_timeout: 6000
    }

    answer = KafkaEx.join_group(request, timeout: 10000, worker_name: client)

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

    answer = KafkaEx.sync_group(request, worker_name: client)
    assert answer.error_code == :no_error

    # Parsing happens to return the assignments reversed, which is fine as there's no
    # ordering. Just reverse what we expect to match
    assert answer.assignments == Enum.reverse(my_assignments)
  end

  test "can leave a consumer group", %{client: client} do
    # A lot of repetition with the previous tests. Leaving it in now, waiting for
    # how this pans out eventually as we add more and more 0.9 consumer group code
    random_group = TestHelper.generate_random_string()

    request = %JoinGroupRequest{
      group_name: random_group,
      member_id: "",
      topics: ["foo", "bar"],
      session_timeout: 6000
    }

    answer = KafkaEx.join_group(request, worker_name: client, timeout: 10000)

    assert answer.error_code == :no_error

    member_id = answer.member_id

    request = %LeaveGroupRequest{
      group_name: random_group,
      member_id: member_id
    }

    answer = KafkaEx.leave_group(request, worker_name: client)
    assert answer.error_code == :no_error
  end

  test "can heartbeat", %{client: client} do
    # See sync test. Removing repetition in the next iteration
    random_group = TestHelper.generate_random_string()

    request = %JoinGroupRequest{
      group_name: random_group,
      member_id: "",
      topics: ["foo", "bar"],
      session_timeout: 6000
    }

    answer = KafkaEx.join_group(request, worker_name: client, timeout: 10000)

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

    answer = KafkaEx.sync_group(request, worker_name: client)
    assert answer.error_code == :no_error

    request = %HeartbeatRequest{
      group_name: random_group,
      member_id: member_id,
      generation_id: generation_id
    }

    answer = KafkaEx.heartbeat(request, worker_name: client)
    assert answer.error_code == :no_error
  end
end
