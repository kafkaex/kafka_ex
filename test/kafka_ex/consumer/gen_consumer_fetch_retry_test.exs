defmodule KafkaEx.Consumer.GenConsumerFetchRetryTest do
  @moduledoc """
  A transient fetch error must not kill the consumer.

  When a partition's leader moves (broker restart, RF=1 makes the window wide),
  the client answers `:no_broker`. Stopping on it takes the whole group down with
  it, because the ConsumerGroup supervisor runs `max_restarts: 0`. brod, KafkaJS
  and librdkafka all back off and retry instead.

  Driven through the real consume loop and the real `KafkaEx.API.fetch/5` reply
  path, so the error is unwrapped by `normalize_reply/1` exactly as in production.
  """
  use ExUnit.Case, async: false

  import KafkaEx.TestSupport.ProcessHelpers

  alias KafkaEx.Client.Error
  alias KafkaEx.Consumer.GenConsumer
  alias KafkaEx.Test.MockClient

  defmodule TestConsumer do
    use KafkaEx.Consumer.GenConsumer
    def handle_message_set(_messages, state), do: {:async_commit, state}
    def handle_call(:ping, _from, state), do: {:reply, :pong, state}
  end

  setup do
    Application.put_env(:kafka_ex, :fetch_retry_base_delay_ms, 10)
    Application.put_env(:kafka_ex, :fetch_retry_max_delay_ms, 40)

    on_exit(fn ->
      Application.delete_env(:kafka_ex, :fetch_retry_base_delay_ms)
      Application.delete_env(:kafka_ex, :fetch_retry_max_delay_ms)
    end)

    :ok
  end

  defp start_consumer(fetch_response) do
    {:ok, client} =
      MockClient.start_link(%{
        offset_fetch: {:ok, [%{partition_offsets: [%{offset: 0, error_code: :no_error}]}]},
        fetch: fetch_response,
        offset_commit: {:ok, []}
      })

    Process.unlink(client)

    {:ok, pid} = GenServer.start_link(GenConsumer, {TestConsumer, "g", "t", 0, [client: client]})
    Process.unlink(pid)

    on_exit(fn ->
      stop_safely(pid)
      stop_safely(client)
    end)

    {client, pid}
  end

  defp fetch_count(client) do
    client |> MockClient.get_calls() |> Enum.count(&match?({:fetch, _, _, _, _}, &1))
  end

  defp wait_until(fun, timeout \\ 2_000) do
    deadline = System.monotonic_time(:millisecond) + timeout
    do_wait_until(fun, deadline)
  end

  defp do_wait_until(fun, deadline) do
    cond do
      fun.() -> :ok
      System.monotonic_time(:millisecond) >= deadline -> flunk("condition not met before deadline")
      true -> Process.sleep(10) && do_wait_until(fun, deadline)
    end
  end

  test "keeps retrying a transient fetch error instead of stopping" do
    {client, pid} = start_consumer({:error, Error.build(:no_broker, %{})})
    ref = Process.monitor(pid)

    refute_receive {:DOWN, ^ref, :process, ^pid, _}, 500

    assert fetch_count(client) >= 3
  end

  test "a call served during backoff does not strand the consumer" do
    {client, pid} = start_consumer({:error, Error.build(:no_broker, %{})})
    wait_until(fn -> fetch_count(client) >= 2 end)

    before = fetch_count(client)
    assert GenConsumer.call(pid, :ping) == :pong

    wait_until(fn -> fetch_count(client) > before + 1 end)
  end

  test "still stops on a fetch error that is not retryable" do
    {_client, pid} = start_consumer({:error, Error.build(:topic_authorization_failed, %{})})
    ref = Process.monitor(pid)

    assert_receive {:DOWN, ^ref, :process, ^pid, :topic_authorization_failed}, 2_000
  end

  test "recovers and resumes consuming once the leader is back" do
    {client, pid} = start_consumer({:error, Error.build(:no_broker, %{})})

    wait_until(fn -> fetch_count(client) >= 3 end)

    MockClient.put_response(client, :fetch, {:ok, %KafkaEx.Messages.Fetch{topic: "t", partition: 0, records: []}})

    recovered = fetch_count(client)
    wait_until(fn -> fetch_count(client) > recovered + 2 end)
    assert Process.alive?(pid)
  end

  test "backs off instead of crashing when the offset reset cannot reach a leader" do
    {:ok, client} =
      MockClient.start_link(%{
        offset_fetch: {:ok, [%{partition_offsets: [%{offset: 0, error_code: :no_error}]}]},
        fetch: {:error, Error.build(:offset_out_of_range, %{})},
        list_offsets: {:error, Error.build(:no_broker, %{})},
        offset_commit: {:ok, []}
      })

    Process.unlink(client)

    {:ok, pid} = GenServer.start_link(GenConsumer, {TestConsumer, "g", "t", 0, [client: client]})
    Process.unlink(pid)

    on_exit(fn ->
      stop_safely(pid)
      stop_safely(client)
    end)

    ref = Process.monitor(pid)

    refute_receive {:DOWN, ^ref, :process, ^pid, _}, 500

    assert fetch_count(client) >= 3
  end
end
