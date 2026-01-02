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
        heartbeat_interval: 5000
      }

      {:ok, state, timeout} = Heartbeat.init(init_args)

      assert state.group_name == "test-group"
      assert state.member_id == "member-1"
      assert state.generation_id == 1
      assert state.client == client
      assert state.heartbeat_interval == 5000
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

    test "stops with rebalance on unknown_member_id error" do
      {:ok, client} = MockClient.start_link(%{heartbeat: {:error, :unknown_member_id}})

      state = %Heartbeat.State{
        client: client,
        group_name: "test-group",
        member_id: "member-1",
        generation_id: 1,
        heartbeat_interval: 1000
      }

      assert {:stop, {:shutdown, :rebalance}, ^state} = Heartbeat.handle_info(:timeout, state)
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
  end
end
