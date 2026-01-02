defmodule KafkaEx.Consumer.GenConsumer.SupervisorTest do
  use ExUnit.Case, async: true

  alias KafkaEx.Consumer.GenConsumer.Supervisor, as: GenConsumerSupervisor

  # Mock GenConsumer module for testing
  defmodule MockGenConsumer do
    use GenServer

    def start_link(_consumer_module, _group_name, _topic, _partition, _opts) do
      GenServer.start_link(__MODULE__, [])
    end

    def init(_), do: {:ok, %{}}
  end

  # Mock consumer module
  defmodule MockConsumer do
  end

  describe "init/1" do
    test "initializes with one_for_one strategy" do
      # The init callback is called by DynamicSupervisor.start_link internally
      # We verify the behavior through the supervisor structure
      assert {:ok, flags} = GenConsumerSupervisor.init({})
      assert flags.strategy == :one_for_one
    end
  end

  describe "start_link/1 (map version)" do
    test "starts supervisor with valid configuration" do
      config = %{
        gen_consumer_module: MockGenConsumer,
        consumer_module: MockConsumer,
        group_name: "test-group",
        assignments: [{"test-topic", 0}],
        opts: []
      }

      assert {:ok, pid} = GenConsumerSupervisor.start_link(config)
      assert Process.alive?(pid)

      # Should have started one child
      children = DynamicSupervisor.which_children(pid)
      assert length(children) == 1

      # Cleanup
      Process.exit(pid, :normal)
    end

    test "starts multiple workers for multiple assignments" do
      config = %{
        gen_consumer_module: MockGenConsumer,
        consumer_module: MockConsumer,
        group_name: "test-group",
        assignments: [{"topic1", 0}, {"topic1", 1}, {"topic2", 0}],
        opts: []
      }

      assert {:ok, pid} = GenConsumerSupervisor.start_link(config)

      children = DynamicSupervisor.which_children(pid)
      assert length(children) == 3

      Process.exit(pid, :normal)
    end

    test "starts with empty assignments" do
      config = %{
        gen_consumer_module: MockGenConsumer,
        consumer_module: MockConsumer,
        group_name: "test-group",
        assignments: [],
        opts: []
      }

      assert {:ok, pid} = GenConsumerSupervisor.start_link(config)

      children = DynamicSupervisor.which_children(pid)
      assert length(children) == 0

      Process.exit(pid, :normal)
    end
  end

  describe "start_link/4 (deprecated tuple version)" do
    test "starts supervisor with valid arguments" do
      assert {:ok, pid} =
               GenConsumerSupervisor.start_link(
                 {MockGenConsumer, MockConsumer},
                 "test-group",
                 [{"test-topic", 0}],
                 []
               )

      assert Process.alive?(pid)
      Process.exit(pid, :normal)
    end
  end

  describe "child_pids/1" do
    test "returns list of child pids" do
      config = %{
        gen_consumer_module: MockGenConsumer,
        consumer_module: MockConsumer,
        group_name: "test-group",
        assignments: [{"topic1", 0}, {"topic1", 1}],
        opts: []
      }

      {:ok, pid} = GenConsumerSupervisor.start_link(config)

      child_pids = GenConsumerSupervisor.child_pids(pid)
      assert length(child_pids) == 2
      assert Enum.all?(child_pids, &is_pid/1)

      Process.exit(pid, :normal)
    end

    test "returns empty list when no children" do
      config = %{
        gen_consumer_module: MockGenConsumer,
        consumer_module: MockConsumer,
        group_name: "test-group",
        assignments: [],
        opts: []
      }

      {:ok, pid} = GenConsumerSupervisor.start_link(config)

      assert GenConsumerSupervisor.child_pids(pid) == []

      Process.exit(pid, :normal)
    end
  end

  describe "active?/1" do
    test "returns true when children are alive" do
      config = %{
        gen_consumer_module: MockGenConsumer,
        consumer_module: MockConsumer,
        group_name: "test-group",
        assignments: [{"test-topic", 0}],
        opts: []
      }

      {:ok, pid} = GenConsumerSupervisor.start_link(config)

      assert GenConsumerSupervisor.active?(pid) == true

      Process.exit(pid, :normal)
    end

    test "returns false when no children" do
      config = %{
        gen_consumer_module: MockGenConsumer,
        consumer_module: MockConsumer,
        group_name: "test-group",
        assignments: [],
        opts: []
      }

      {:ok, pid} = GenConsumerSupervisor.start_link(config)

      assert GenConsumerSupervisor.active?(pid) == false

      Process.exit(pid, :normal)
    end
  end
end
