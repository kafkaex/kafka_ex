defmodule KafkaEx.Consumer.GenConsumerInitialTopicsTest do
  @moduledoc """
  When a GenConsumer starts its OWN client (no shared `:client` passed), that
  client must be seeded with `initial_topics: [topic]` so its first metadata
  refresh is scoped to this consumer's topic instead of the whole cluster catalog.
  """
  use ExUnit.Case
  use Mimic

  alias KafkaEx.Client
  alias KafkaEx.Consumer.GenConsumer
  alias KafkaEx.Messages.Fetch
  alias KafkaEx.Messages.Fetch.Record
  alias KafkaEx.Test.MockClient

  defmodule TestConsumer do
    use KafkaEx.Consumer.GenConsumer
    def handle_message_set(_messages, state), do: {:async_commit, state}
  end

  # Global mode: GenConsumer.init runs in the spawned process, not the test process.
  setup :set_mimic_global

  defp mock_client do
    MockClient.start_link(%{
      offset_fetch: {:ok, [%{partition_offsets: [%{offset: 0, error_code: :no_error}]}]},
      fetch:
        {:ok,
         %Fetch{topic: "my-topic", partition: 0, records: [%Record{offset: 0, value: "m", key: nil}], last_offset: 0}},
      offset_commit: {:ok, []}
    })
  end

  defp start_gen_consumer(consumer_opts) do
    {:ok, pid} =
      GenServer.start_link(GenConsumer, {TestConsumer, "my-group", "my-topic", 0, consumer_opts})

    pid
  end

  defp stop(pid) do
    Process.unlink(pid)
    GenServer.stop(pid, :normal, 1_000)
  end

  test "starting its own client seeds initial_topics with this consumer's topic" do
    test_pid = self()
    {:ok, mock} = mock_client()

    stub(Client, :start_link, fn opts, :no_name ->
      send(test_pid, {:client_opts, opts})
      {:ok, mock}
    end)

    pid = start_gen_consumer([])

    assert_receive {:client_opts, opts}, 1_000
    assert Keyword.get(opts, :initial_topics) == ["my-topic"]
    assert Keyword.get(opts, :consumer_group) == "my-group"

    stop(pid)
  end

  test "a caller-supplied shared client is used as-is; no client is started" do
    test_pid = self()
    {:ok, shared} = mock_client()

    stub(Client, :start_link, fn _opts, _name ->
      send(test_pid, :started_own_client)
      {:ok, shared}
    end)

    pid = start_gen_consumer(client: shared)

    refute_receive :started_own_client, 300
    stop(pid)
  end
end
