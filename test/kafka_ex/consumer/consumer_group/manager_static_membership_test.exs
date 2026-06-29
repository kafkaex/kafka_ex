defmodule KafkaEx.Consumer.ConsumerGroup.ManagerStaticMembershipTest do
  @moduledoc """
  KIP-345 static membership wiring through the Manager: option resolution/
  validation, group_instance_id threaded into join/sync/heartbeat, LeaveGroup
  suppression on terminate, and the too-old-broker warning. Exercised through
  public behavior only; no private function is exposed for testing.
  """
  use ExUnit.Case, async: false

  import KafkaEx.TestSupport.ProcessHelpers

  alias KafkaEx.Consumer.ConsumerGroup.Manager
  alias KafkaEx.Consumer.ConsumerGroup.Manager.State

  describe "init/1 group_instance_id resolution" do
    test "a valid group_instance_id opt is stored on the State" do
      {:ok, client} = KafkaEx.Test.MockClient.start_link(%{})
      on_exit(fn -> stop_safely(client) end)

      opts = [supervisor_pid: self(), client: client, group_instance_id: "inst-1"]

      {:ok, state, _timeout} =
        Manager.init({{KafkaEx.GenConsumer, __MODULE__.NoopConsumer}, "g", ["t"], opts})

      assert %State{group_instance_id: "inst-1"} = state
    end

    test "no group_instance_id opt leaves it nil (dynamic membership)" do
      {:ok, client} = KafkaEx.Test.MockClient.start_link(%{})
      on_exit(fn -> stop_safely(client) end)

      opts = [supervisor_pid: self(), client: client]

      {:ok, state, _timeout} =
        Manager.init({{KafkaEx.GenConsumer, __MODULE__.NoopConsumer}, "g", ["t"], opts})

      assert %State{group_instance_id: nil} = state
    end

    test "a blank group_instance_id opt raises ArgumentError" do
      {:ok, client} = KafkaEx.Test.MockClient.start_link(%{})
      on_exit(fn -> stop_safely(client) end)

      opts = [supervisor_pid: self(), client: client, group_instance_id: ""]

      assert_raise ArgumentError, ~r/non-empty/, fn ->
        Manager.init({{KafkaEx.GenConsumer, __MODULE__.NoopConsumer}, "g", ["t"], opts})
      end
    end
  end

  defmodule NoopConsumer do
    @moduledoc false
  end
end
