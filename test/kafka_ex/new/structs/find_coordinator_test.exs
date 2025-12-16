defmodule KafkaEx.New.Kafka.FindCoordinatorTest do
  use ExUnit.Case, async: true

  alias KafkaEx.New.Kafka.Broker
  alias KafkaEx.New.Kafka.FindCoordinator

  describe "build/1" do
    test "creates struct with default values" do
      result = FindCoordinator.build()

      assert result.coordinator == nil
      assert result.error_code == :no_error
      assert result.error_message == nil
      assert result.throttle_time_ms == nil
    end

    test "creates struct with all fields" do
      coordinator = %Broker{node_id: 1, host: "localhost", port: 9092}

      result =
        FindCoordinator.build(
          coordinator: coordinator,
          error_code: :no_error,
          error_message: "some message",
          throttle_time_ms: 100
        )

      assert result.coordinator == coordinator
      assert result.error_code == :no_error
      assert result.error_message == "some message"
      assert result.throttle_time_ms == 100
    end

    test "creates struct with error" do
      result =
        FindCoordinator.build(
          coordinator: nil,
          error_code: :coordinator_not_available,
          error_message: "Coordinator not available"
        )

      assert result.coordinator == nil
      assert result.error_code == :coordinator_not_available
      assert result.error_message == "Coordinator not available"
    end
  end

  describe "coordinator_type_to_int/1" do
    test "converts :group to 0" do
      assert FindCoordinator.coordinator_type_to_int(:group) == 0
    end

    test "converts :transaction to 1" do
      assert FindCoordinator.coordinator_type_to_int(:transaction) == 1
    end
  end

  describe "int_to_coordinator_type/1" do
    test "converts 0 to :group" do
      assert FindCoordinator.int_to_coordinator_type(0) == :group
    end

    test "converts 1 to :transaction" do
      assert FindCoordinator.int_to_coordinator_type(1) == :transaction
    end
  end

  describe "success?/1" do
    test "returns true when error_code is :no_error" do
      result = FindCoordinator.build(error_code: :no_error)
      assert FindCoordinator.success?(result)
    end

    test "returns false when error_code is not :no_error" do
      result = FindCoordinator.build(error_code: :coordinator_not_available)
      refute FindCoordinator.success?(result)
    end
  end

  describe "coordinator_node_id/1" do
    test "returns node_id when coordinator is present" do
      coordinator = %Broker{node_id: 42, host: "localhost", port: 9092}
      result = FindCoordinator.build(coordinator: coordinator)

      assert FindCoordinator.coordinator_node_id(result) == 42
    end

    test "returns nil when coordinator is nil" do
      result = FindCoordinator.build(coordinator: nil)

      assert FindCoordinator.coordinator_node_id(result) == nil
    end
  end
end
