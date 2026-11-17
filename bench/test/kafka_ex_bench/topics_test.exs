defmodule KafkaExBench.TopicsTest do
  use ExUnit.Case, async: true
  alias KafkaEx.Messages.CreateTopics
  alias KafkaEx.Messages.CreateTopics.TopicResult
  alias KafkaExBench.Topics

  defp result(error), do: %CreateTopics{topic_results: [%TopicResult{topic: "t1", error: error}]}

  test "ensure_distinct_leaders! accepts one leader per partition" do
    assert Topics.ensure_distinct_leaders!(%{0 => 1, 1 => 2, 2 => 3}) == :ok
  end

  test "ensure_distinct_leaders! raises showing the leader map when brokers repeat" do
    error = assert_raise RuntimeError, fn -> Topics.ensure_distinct_leaders!(%{0 => 1, 1 => 1, 2 => 3}) end
    assert error.message =~ inspect(%{0 => 1, 1 => 1, 2 => 3})
  end

  test "ensure_created! passes on success" do
    assert Topics.ensure_created!(result(:no_error), "t1") == :ok
  end

  test "ensure_created! raises naming the topic and the failure" do
    error = assert_raise RuntimeError, fn -> Topics.ensure_created!(result(:topic_already_exists), "t1") end
    assert error.message =~ "t1"
    assert error.message =~ "topic_already_exists"
  end

  test "ensure_deleted! passes on success and raises naming the topic on failure" do
    ok = %KafkaEx.Messages.DeleteTopics{
      topic_results: [%KafkaEx.Messages.DeleteTopics.TopicResult{topic: "t1", error: :no_error}]
    }

    bad = %KafkaEx.Messages.DeleteTopics{
      topic_results: [%KafkaEx.Messages.DeleteTopics.TopicResult{topic: "t1", error: :unknown_topic_or_partition}]
    }

    assert Topics.ensure_deleted!(ok, "t1") == :ok
    error = assert_raise RuntimeError, fn -> Topics.ensure_deleted!(bad, "t1") end
    assert error.message =~ "t1"
    assert error.message =~ "unknown_topic_or_partition"
  end

  describe "create_options/1" do
    test "partitions and replication factor go to the broker as given, with the factor defaulting to 3" do
      assert Topics.create_options(partitions: 6) ==
               [num_partitions: 6, replication_factor: 3, timeout: 30_000]

      assert Topics.create_options(partitions: 6, replication_factor: 2)[:replication_factor] == 2
    end

    test "an explicit replica assignment replaces partition count and factor with -1" do
      assignment = [{0, [1, 2, 3]}, {1, [1, 3, 2]}]

      assert Topics.create_options(partitions: 2, replication_factor: 3, replica_assignment: assignment) ==
               [num_partitions: -1, replication_factor: -1, replica_assignment: assignment, timeout: 30_000]
    end
  end

  describe "cleanup_on_failure/3" do
    setup do
      dead = spawn(fn -> :ok end)
      Process.sleep(20)
      {:ok, dead: dead}
    end

    defp quietly(fun), do: ExUnit.CaptureIO.capture_io(:stderr, fun)

    test "returns the value and does not delete on success", %{dead: dead} do
      output = quietly(fn -> send(self(), {:value, Topics.cleanup_on_failure(dead, "t1", fn -> 42 end)}) end)

      assert_received {:value, 42}
      assert output == ""
    end

    test "deletes and re-raises an exception", %{dead: dead} do
      output =
        quietly(fn ->
          assert_raise RuntimeError, "boom", fn -> Topics.cleanup_on_failure(dead, "t1", fn -> raise "boom" end) end
        end)

      assert output =~ "could not delete topic t1"
    end

    test "deletes and re-exits with the original reason", %{dead: dead} do
      output =
        quietly(fn ->
          assert catch_exit(Topics.cleanup_on_failure(dead, "t1", fn -> exit(:timeout) end)) == :timeout
        end)

      assert output =~ "could not delete topic t1"
    end

    test "deletes and re-throws the original value", %{dead: dead} do
      output =
        quietly(fn -> assert catch_throw(Topics.cleanup_on_failure(dead, "t1", fn -> throw(:oops) end)) == :oops end)

      assert output =~ "could not delete topic t1"
    end
  end

  describe "classify/2" do
    test "a topic missing from the metadata is absent" do
      assert Topics.classify(%{topics: %{}}, "t1") == :absent
      assert Topics.classify(%{topics: %{"other" => %{partition_leaders: %{0 => 1}}}}, "t1") == :absent
    end

    test "a present topic yields its layout even with a missing leader" do
      metadata = %{topics: %{"t1" => %{partition_leaders: %{0 => 1, 1 => -1}}}}

      assert Topics.classify(metadata, "t1") == {:layout, %{0 => 1, 1 => -1}}
    end
  end

  test "cleanup after a failure warns instead of raising when the client exits" do
    dead = spawn(fn -> :ok end)
    Process.sleep(20)

    warning = ExUnit.CaptureIO.capture_io(:stderr, fn -> assert Topics.delete_after_failure(dead, "t1") end)
    assert warning =~ "could not delete topic t1"
  end
end
