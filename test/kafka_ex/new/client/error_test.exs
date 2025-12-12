defmodule KafkaEx.New.Client.ErrorTest do
  use ExUnit.Case, async: true

  alias KafkaEx.New.Client.Error

  describe "build/2 with integer error code" do
    test "builds error with no_error code (0)" do
      result = Error.build(0, %{})
      assert %Error{error: :no_error, metadata: %{}} = result
    end

    test "builds error with unknown_topic_or_partition code (3)" do
      metadata = %{topic: "test-topic", partition: 0}
      result = Error.build(3, metadata)
      assert result.error == :unknown_topic_or_partition
      assert result.metadata == metadata
    end

    test "builds error with leader_not_available code (5)" do
      result = Error.build(5, %{topic: "topic"})
      assert result.error == :leader_not_available
    end

    test "builds error with not_leader_for_partition code (6)" do
      result = Error.build(6, %{})
      assert result.error == :not_leader_for_partition
    end

    test "builds error with request_timed_out code (7)" do
      result = Error.build(7, %{})
      assert result.error == :request_timed_out
    end

    test "builds error with offset_metadata_too_large code (12)" do
      result = Error.build(12, %{})
      assert result.error == :offset_metadata_too_large
    end

    test "builds error with coordinator_load_in_progress code (14)" do
      result = Error.build(14, %{})
      assert result.error == :coordinator_load_in_progress
    end

    test "builds error with coordinator_not_available code (15)" do
      result = Error.build(15, %{consumer_group: "group"})
      assert result.error == :coordinator_not_available
      assert result.metadata == %{consumer_group: "group"}
    end

    test "builds error with not_coordinator code (16)" do
      result = Error.build(16, %{})
      assert result.error == :not_coordinator
    end

    test "builds error with illegal_generation code (22)" do
      result = Error.build(22, %{generation_id: 5})
      assert result.error == :illegal_generation
    end

    test "builds error with unknown_member_id code (25)" do
      result = Error.build(25, %{member_id: "member-123"})
      assert result.error == :unknown_member_id
      assert result.metadata.member_id == "member-123"
    end

    test "builds error with rebalance_in_progress code (27)" do
      result = Error.build(27, %{})
      assert result.error == :rebalance_in_progress
    end

    test "builds error with group_authorization_failed code (30)" do
      result = Error.build(30, %{})
      assert result.error == :group_authorization_failed
    end

    test "builds error with topic_authorization_failed code (29)" do
      result = Error.build(29, %{topic: "secret-topic"})
      assert result.error == :topic_authorization_failed
    end

    test "builds error with group_id_not_found code (69)" do
      result = Error.build(69, %{})
      assert result.error == :group_id_not_found
    end

    test "builds error with unknown error code" do
      result = Error.build(999, %{})
      assert result.error == :unknown
    end
  end

  describe "build/2 with atom error" do
    test "builds error with atom directly" do
      result = Error.build(:custom_error, %{})
      assert %Error{error: :custom_error, metadata: %{}} = result
    end

    test "builds error with empty_response atom" do
      result = Error.build(:empty_response, %{})
      assert result.error == :empty_response
    end

    test "builds error with timeout atom" do
      result = Error.build(:timeout, %{request_type: :produce})
      assert result.error == :timeout
      assert result.metadata.request_type == :produce
    end

    test "builds error with connection_refused atom" do
      result = Error.build(:connection_refused, %{host: "localhost", port: 9092})
      assert result.error == :connection_refused
      assert result.metadata == %{host: "localhost", port: 9092}
    end

    test "preserves arbitrary atom errors" do
      result = Error.build(:my_custom_error, %{custom: "data"})
      assert result.error == :my_custom_error
      assert result.metadata == %{custom: "data"}
    end
  end

  describe "build/2 metadata handling" do
    test "preserves map metadata" do
      metadata = %{topic: "t", partition: 0, extra: "info"}
      result = Error.build(:some_error, metadata)
      assert result.metadata == metadata
    end

    test "preserves empty map metadata" do
      result = Error.build(:error, %{})
      assert result.metadata == %{}
    end

    test "preserves keyword list metadata" do
      metadata = [topic: "t", partition: 0]
      result = Error.build(:error, metadata)
      assert result.metadata == metadata
    end

    test "preserves nil metadata" do
      result = Error.build(:error, nil)
      assert result.metadata == nil
    end

    test "preserves nested metadata" do
      metadata = %{
        topic: "topic",
        details: %{
          code: 42,
          message: "Something happened"
        }
      }

      result = Error.build(:complex_error, metadata)
      assert result.metadata == metadata
    end
  end

  describe "struct behavior" do
    test "can pattern match on struct" do
      error = Error.build(:test_error, %{})
      assert %Error{error: :test_error} = error
    end

    test "can access fields directly" do
      error = Error.build(:test, %{key: "value"})
      assert error.error == :test
      assert error.metadata == %{key: "value"}
    end

    test "default values" do
      # Creating struct directly without build
      error = %Error{}
      assert error.error == nil
      assert error.metadata == %{}
    end
  end
end
