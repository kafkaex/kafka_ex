defmodule KafkaEx.Support.ExceptionsTest do
  use ExUnit.Case, async: true

  # Define test struct outside of test block
  defmodule TestRequest do
    defstruct []
  end

  describe "KafkaEx.ConsumerGroupRequiredError" do
    test "formats message with struct type" do
      exception = KafkaEx.ConsumerGroupRequiredError.exception(%TestRequest{})

      assert exception.message =~ "TestRequest require"
      assert exception.message =~ "configured for a consumer group"
    end

    test "formats message with binary action" do
      exception = KafkaEx.ConsumerGroupRequiredError.exception("offset commit")

      assert exception.message == "KafkaEx offset commit requires that the worker is configured for a consumer group."
    end

    test "can be raised and caught" do
      assert_raise KafkaEx.ConsumerGroupRequiredError, fn ->
        raise KafkaEx.ConsumerGroupRequiredError, "test action"
      end
    end
  end

  describe "KafkaEx.InvalidConsumerGroupError" do
    test "formats message with consumer group" do
      exception = KafkaEx.InvalidConsumerGroupError.exception("bad-group")

      assert exception.message == "Invalid consumer group: \"bad-group\""
    end

    test "formats message with nil consumer group" do
      exception = KafkaEx.InvalidConsumerGroupError.exception(nil)

      assert exception.message == "Invalid consumer group: nil"
    end

    test "formats message with empty string" do
      exception = KafkaEx.InvalidConsumerGroupError.exception("")

      assert exception.message == "Invalid consumer group: \"\""
    end

    test "can be raised and caught" do
      assert_raise KafkaEx.InvalidConsumerGroupError, fn ->
        raise KafkaEx.InvalidConsumerGroupError, "invalid"
      end
    end
  end

  describe "KafkaEx.TimestampNotSupportedError" do
    test "has default message" do
      exception = %KafkaEx.TimestampNotSupportedError{}

      assert exception.message == "Timestamp requires produce api_version >= 3"
    end

    test "can be raised with default message" do
      assert_raise KafkaEx.TimestampNotSupportedError, "Timestamp requires produce api_version >= 3", fn ->
        raise KafkaEx.TimestampNotSupportedError
      end
    end

    test "can be raised and caught" do
      assert_raise KafkaEx.TimestampNotSupportedError, fn ->
        raise KafkaEx.TimestampNotSupportedError
      end
    end
  end

  describe "KafkaEx.JoinGroupError" do
    test "formats message with group name and reason" do
      exception = KafkaEx.JoinGroupError.exception(group_name: "my-group", reason: :illegal_generation)

      assert exception.message == "Error joining consumer group my-group: :illegal_generation"
      assert exception.group_name == "my-group"
      assert exception.reason == :illegal_generation
    end

    test "can be raised and caught" do
      assert_raise KafkaEx.JoinGroupError, ~r/Error joining consumer group/, fn ->
        raise KafkaEx.JoinGroupError, group_name: "test-group", reason: :unknown_member_id
      end
    end

    test "exception struct contains all fields" do
      exception = KafkaEx.JoinGroupError.exception(group_name: "g", reason: :error)

      assert %KafkaEx.JoinGroupError{} = exception
      assert Map.has_key?(exception, :message)
      assert Map.has_key?(exception, :group_name)
      assert Map.has_key?(exception, :reason)
    end
  end

  describe "KafkaEx.JoinGroupRetriesExhaustedError" do
    test "formats message with all details" do
      exception =
        KafkaEx.JoinGroupRetriesExhaustedError.exception(
          group_name: "my-group",
          last_error: :coordinator_not_available,
          attempts: 6
        )

      assert exception.message =~ "Unable to join consumer group my-group"
      assert exception.message =~ "after 6 attempts"
      assert exception.message =~ ":coordinator_not_available"
      assert exception.group_name == "my-group"
      assert exception.last_error == :coordinator_not_available
      assert exception.attempts == 6
    end

    test "can be raised and caught" do
      assert_raise KafkaEx.JoinGroupRetriesExhaustedError, ~r/after \d+ attempts/, fn ->
        raise KafkaEx.JoinGroupRetriesExhaustedError,
          group_name: "test-group",
          last_error: :timeout,
          attempts: 5
      end
    end

    test "exception struct contains all fields" do
      exception =
        KafkaEx.JoinGroupRetriesExhaustedError.exception(
          group_name: "g",
          last_error: :e,
          attempts: 1
        )

      assert %KafkaEx.JoinGroupRetriesExhaustedError{} = exception
      assert Map.has_key?(exception, :message)
      assert Map.has_key?(exception, :group_name)
      assert Map.has_key?(exception, :last_error)
      assert Map.has_key?(exception, :attempts)
    end
  end

  describe "KafkaEx.SyncGroupError" do
    test "formats message with group name and reason" do
      exception = KafkaEx.SyncGroupError.exception(group_name: "my-group", reason: :unknown_member_id)

      assert exception.message == "Error syncing consumer group my-group: :unknown_member_id"
      assert exception.group_name == "my-group"
      assert exception.reason == :unknown_member_id
    end

    test "can be raised and caught" do
      assert_raise KafkaEx.SyncGroupError, ~r/Error syncing consumer group/, fn ->
        raise KafkaEx.SyncGroupError, group_name: "test-group", reason: :illegal_generation
      end
    end

    test "exception struct contains all fields" do
      exception = KafkaEx.SyncGroupError.exception(group_name: "g", reason: :error)

      assert %KafkaEx.SyncGroupError{} = exception
      assert Map.has_key?(exception, :message)
      assert Map.has_key?(exception, :group_name)
      assert Map.has_key?(exception, :reason)
    end
  end
end
