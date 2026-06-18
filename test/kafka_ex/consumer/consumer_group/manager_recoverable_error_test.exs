defmodule KafkaEx.Consumer.ConsumerGroup.ManagerRecoverableErrorTest do
  @moduledoc """
  Regression for PR-2: once the client preserves the real transport atom (C-1), a
  TCP-level coordinator failure reaches the manager as `:closed` / `:not_connected`
  instead of `:unknown`. The heartbeat-exit handler must treat those as recoverable
  (-> rejoin), not stop the manager — previously they arrived as `:unknown`, which
  was already recoverable. Exercised through the real `handle_info/2` path; no
  private function is exposed for testing.

  Flagged independently by the Kafka-protocol and Java-client code reviews.
  """
  use ExUnit.Case, async: false

  alias KafkaEx.Consumer.ConsumerGroup.Manager
  alias KafkaEx.Consumer.ConsumerGroup.Manager.State
  alias KafkaEx.Test.MockClient

  defp heartbeat_exit(reason), do: {:EXIT, make_ref(), {:shutdown, {:error, reason}}}

  test "a fatal heartbeat error stops the manager" do
    state = %State{group_name: "g", member_id: "m", generation_id: 1}

    assert {:stop, {:shutdown, {:error, :topic_authorization_failed}}, ^state} =
             Manager.handle_info(heartbeat_exit(:topic_authorization_failed), state)
  end

  test "a :closed transport error is recoverable and drives a rejoin, not a stop" do
    # join_group fails non-recoverably so the rejoin raises immediately (no retry
    # backoff sleeps, no consumer startup). Reaching join at all proves :closed was
    # routed to the recoverable rejoin path rather than {:stop, ...}.
    {:ok, client} = MockClient.start_link(%{join_group: {:error, :illegal_generation}})
    on_exit(fn -> if Process.alive?(client), do: GenServer.stop(client) end)

    {:ok, sup} = Supervisor.start_link([], strategy: :one_for_one)
    on_exit(fn -> if Process.alive?(sup), do: Supervisor.stop(sup) end)

    state = %State{
      client: client,
      supervisor_pid: sup,
      group_name: "g",
      topics: ["t"],
      member_id: "m",
      generation_id: 1,
      session_timeout: 1000,
      session_timeout_padding: 0,
      rebalance_timeout: 1000,
      heartbeat_timer: nil
    }

    assert_raise KafkaEx.JoinGroupError, fn ->
      Manager.handle_info(heartbeat_exit(:closed), state)
    end

    # the recoverable path actually attempted a rejoin against the coordinator
    assert {:join_group, "g", "m"} in MockClient.get_calls(client)
  end
end
