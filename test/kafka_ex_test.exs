defmodule KafkaExTest do
  use ExUnit.Case, async: true

  describe "build_worker_options/1" do
    test "translates :brokers to :uris" do
      {:ok, opts} = KafkaEx.build_worker_options(brokers: [{"example", 9092}])

      assert opts[:uris] == [{"example", 9092}]
      refute Keyword.has_key?(opts, :brokers)
    end

    test "an explicit :uris wins when both :brokers and :uris are given" do
      {:ok, opts} = KafkaEx.build_worker_options(uris: [{"u", 1}], brokers: [{"b", 2}])

      assert opts[:uris] == [{"u", 1}]
      refute Keyword.has_key?(opts, :brokers)
    end

    test "user :brokers overrides the config-derived default uris" do
      {:ok, opts} = KafkaEx.build_worker_options(brokers: [{"custom", 1234}])

      assert opts[:uris] == [{"custom", 1234}]
    end

    test "merges config defaults when nothing is supplied" do
      {:ok, opts} = KafkaEx.build_worker_options([])

      # config/config.exs sets default_consumer_group: "kafka_ex"
      assert opts[:consumer_group] == "kafka_ex"
      assert is_list(opts[:uris])
    end

    test "returns error for an invalid (empty) consumer group" do
      assert {:error, :invalid_consumer_group} =
               KafkaEx.build_worker_options(consumer_group: "")
    end

    test "is idempotent — feeding its own output back in is a no-op" do
      {:ok, once} = KafkaEx.build_worker_options(brokers: [{"x", 1}])
      {:ok, twice} = KafkaEx.build_worker_options(once)

      assert Enum.sort(once) == Enum.sort(twice)
    end
  end
end
