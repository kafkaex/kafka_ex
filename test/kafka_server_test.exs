defmodule Kafka.Server.Test do
  use ExUnit.Case
  import Mock

  @brokers %{0 => {"broker0", 9092}, 1 => {"broker1", 9092}}
  @topics  %{"test" => %{:error_code => 0,
                         :partitions =>
                           %{0 => %{:error_code => 0, :isrs => [1], :leader => 0, :replicas => [1]},
                             1 => %{:error_code => 0, :isrs => [0], :leader => 1, :replicas => [0]}}}}

  test "server connects to leader for topic/partition on produce" do
    with_mock :gen_tcp, [:unstick], [connect: fn(h, p, o) -> {:ok, {1, @brokers, @topics}} end,
                                     send: fn(s, m) -> TestHelper.mock_send(s, m) end] do
      Kafka.Server.start_link([{"broker0", 9092}])
      Kafka.Server.produce("test", 1, "Message")
      assert called :gen_tcp.connect('broker1', 9092, [:binary, {:packet, 4}])
    end
  end

  test "server connects to leader for topic/partition on fetch" do
    with_mock :gen_tcp, [:unstick], [connect: fn(h, p, _) -> {:ok, {1, @brokers, @topics}} end,
                                     send: fn(s, m) -> TestHelper.mock_send(s, m) end] do
      Kafka.Server.start_link([{"broker0", 9092}])
      Kafka.Server.fetch("test", 1, 0)
      assert called :gen_tcp.connect('broker1', 9092, [:binary, {:packet, 4}])
    end
  end
end
