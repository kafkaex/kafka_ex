defmodule KafkaEx.Consumer.ConsumerGroup.HeartbeatTest do
  use ExUnit.Case, async: true

  alias KafkaEx.Consumer.ConsumerGroup.Heartbeat
  alias KafkaEx.Messages.Heartbeat, as: HeartbeatResponse
  alias KafkaEx.Test.MockClient

  describe "init/1" do
    test "initializes state with heartbeat interval timeout" do
      {:ok, client} = MockClient.start_link(%{})

      init_args = %{
        group_name: "test-group",
        member_id: "member-1",
        generation_id: 1,
        client: client,
        heartbeat_interval: 5000,
        group_instance_id: nil
      }

      {:ok, state, timeout} = Heartbeat.init(init_args)

      assert state.group_name == "test-group"
      assert state.member_id == "member-1"
      assert state.generation_id == 1
      assert state.client == client
      assert state.heartbeat_interval == 5000
      assert state.group_instance_id == nil
      assert timeout == 5000
    end
  end

  describe "handle_info(:timeout, state)" do
    test "continues with heartbeat interval on successful heartbeat" do
      {:ok, client} = MockClient.start_link(%{heartbeat: {:ok, %HeartbeatResponse{}}})

      state = %Heartbeat.State{
        client: client,
        group_name: "test-group",
        member_id: "member-1",
        generation_id: 1,
        heartbeat_interval: 1000
      }

      assert {:noreply, ^state, 1000} = Heartbeat.handle_info(:timeout, state)
    end

    test "continues with heartbeat interval on successful heartbeat with throttle_time" do
      {:ok, client} = MockClient.start_link(%{heartbeat: {:ok, %HeartbeatResponse{throttle_time_ms: 100}}})

      state = %Heartbeat.State{
        client: client,
        group_name: "test-group",
        member_id: "member-1",
        generation_id: 1,
        heartbeat_interval: 2000
      }

      assert {:noreply, ^state, 2000} = Heartbeat.handle_info(:timeout, state)
    end

    test "stops with rebalance on rebalance_in_progress error" do
      {:ok, client} = MockClient.start_link(%{heartbeat: {:error, :rebalance_in_progress}})

      state = %Heartbeat.State{
        client: client,
        group_name: "test-group",
        member_id: "member-1",
        generation_id: 1,
        heartbeat_interval: 1000
      }

      assert {:stop, {:shutdown, :rebalance}, ^state} = Heartbeat.handle_info(:timeout, state)
    end

    test "stops with rejoin(:unknown_member_id) so the manager resets member_id and rejoins" do
      {:ok, client} = MockClient.start_link(%{heartbeat: {:error, :unknown_member_id}})

      state = %Heartbeat.State{
        client: client,
        group_name: "test-group",
        member_id: "member-1",
        generation_id: 1,
        heartbeat_interval: 1000
      }

      assert {:stop, {:shutdown, {:rejoin, :unknown_member_id}}, ^state} =
               Heartbeat.handle_info(:timeout, state)
    end

    test "stops with rejoin(:illegal_generation) so the manager resets generation and rejoins" do
      {:ok, client} = MockClient.start_link(%{heartbeat: {:error, :illegal_generation}})

      state = %Heartbeat.State{
        client: client,
        group_name: "test-group",
        member_id: "member-1",
        generation_id: 1,
        heartbeat_interval: 1000
      }

      assert {:stop, {:shutdown, {:rejoin, :illegal_generation}}, ^state} =
               Heartbeat.handle_info(:timeout, state)
    end

    test "stops terminally on fenced_instance_id (static membership conflict, no rejoin)" do
      {:ok, client} = MockClient.start_link(%{heartbeat: {:error, :fenced_instance_id}})

      state = %Heartbeat.State{
        client: client,
        group_name: "test-group",
        member_id: "member-1",
        generation_id: 1,
        heartbeat_interval: 1000
      }

      assert {:stop, {:shutdown, {:terminal, :fenced_instance_id}}, ^state} =
               Heartbeat.handle_info(:timeout, state)
    end

    test "stops terminally on group_authorization_failed (no group access, no rejoin)" do
      {:ok, client} = MockClient.start_link(%{heartbeat: {:error, :group_authorization_failed}})

      state = %Heartbeat.State{
        client: client,
        group_name: "test-group",
        member_id: "member-1",
        generation_id: 1,
        heartbeat_interval: 1000
      }

      assert {:stop, {:shutdown, {:terminal, :group_authorization_failed}}, ^state} =
               Heartbeat.handle_info(:timeout, state)
    end

    test "stops with error on other error codes" do
      {:ok, client} = MockClient.start_link(%{heartbeat: {:error, :coordinator_not_available}})

      state = %Heartbeat.State{
        client: client,
        group_name: "test-group",
        member_id: "member-1",
        generation_id: 1,
        heartbeat_interval: 1000
      }

      assert {:stop, {:shutdown, {:error, :coordinator_not_available}}, ^state} = Heartbeat.handle_info(:timeout, state)
    end

    test "stops with error on network_error" do
      {:ok, client} = MockClient.start_link(%{heartbeat: {:error, :network_error}})

      state = %Heartbeat.State{
        client: client,
        group_name: "test-group",
        member_id: "member-1",
        generation_id: 1,
        heartbeat_interval: 1000
      }

      assert {:stop, {:shutdown, {:error, :network_error}}, ^state} = Heartbeat.handle_info(:timeout, state)
    end

    test "an unexpected response shape is rescued into a terminal {:crashed, _} stop" do
      # API.heartbeat/4 pattern-matches the client reply in a case; an unknown
      # shape raises CaseClauseError inside the heartbeat process. The wrapper
      # must convert that code-defect crash into a clean terminal stop, not let
      # it escape as a bare abnormal exit.
      {:ok, client} = MockClient.start_link(%{heartbeat: :unexpected_garbage})

      state = %Heartbeat.State{
        client: client,
        group_name: "test-group",
        member_id: "member-1",
        generation_id: 1,
        heartbeat_interval: 1000
      }

      assert {:stop, {:shutdown, {:terminal, {:crashed, CaseClauseError}}}, ^state} =
               Heartbeat.handle_info(:timeout, state)
    end

    test "a client-call exit is caught and converted to a structured {:error, _} stop" do
      # A dead client makes API.heartbeat's GenServer.call exit
      # {:noproc, {GenServer, :call, _}}. The wrapper must catch it and exit with
      # a structured {:shutdown, {:error, :noproc}} the Manager can route, rather
      # than dying with a bare abnormal reason the Manager can't match.
      {:ok, client} = MockClient.start_link(%{})
      Process.unlink(client)
      ref = Process.monitor(client)
      Process.exit(client, :kill)

      receive do
        {:DOWN, ^ref, :process, ^client, _} -> :ok
      end

      state = %Heartbeat.State{
        client: client,
        group_name: "test-group",
        member_id: "member-1",
        generation_id: 1,
        heartbeat_interval: 1000
      }

      assert {:stop, {:shutdown, {:error, :noproc}}, ^state} =
               Heartbeat.handle_info(:timeout, state)
    end
  end
end
