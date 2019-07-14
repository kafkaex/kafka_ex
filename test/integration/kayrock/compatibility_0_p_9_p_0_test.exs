defmodule KafkaEx.KayrockCompatibilityTest do
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
end
