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
end
