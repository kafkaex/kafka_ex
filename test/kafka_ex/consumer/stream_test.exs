defmodule KafkaEx.Consumer.StreamTest do
  use ExUnit.Case, async: true

  alias KafkaEx.Consumer.Stream

  describe "struct" do
    test "has default values" do
      stream = %Stream{}

      assert stream.client == nil
      assert stream.topic == nil
      assert stream.partition == nil
      assert stream.offset == 0
      assert stream.consumer_group == nil
      assert stream.no_wait_at_logend == false
      assert stream.fetch_options == []
      assert stream.api_versions == %{}
    end

    test "can be initialized with values" do
      stream = %Stream{
        client: :test_client,
        topic: "test-topic",
        partition: 0,
        offset: 100,
        consumer_group: "test-group",
        no_wait_at_logend: true,
        fetch_options: [max_bytes: 1024],
        api_versions: %{fetch: 3, offset_fetch: 1, offset_commit: 2}
      }

      assert stream.client == :test_client
      assert stream.topic == "test-topic"
      assert stream.partition == 0
      assert stream.offset == 100
      assert stream.consumer_group == "test-group"
      assert stream.no_wait_at_logend == true
      assert stream.fetch_options == [max_bytes: 1024]
      assert stream.api_versions == %{fetch: 3, offset_fetch: 1, offset_commit: 2}
    end
  end

  describe "api_versions defaults" do
    test "defaults to empty map (broker decides versions)" do
      stream = %Stream{}
      assert stream.api_versions == %{}
    end

    test "explicit versions are preserved" do
      stream = %Stream{api_versions: %{fetch: 5, offset_commit: 3}}
      assert stream.api_versions == %{fetch: 5, offset_commit: 3}
    end

    test "partial versions leave other keys absent" do
      stream = %Stream{api_versions: %{fetch: 2}}
      assert Map.get(stream.api_versions, :fetch) == 2
      assert Map.get(stream.api_versions, :offset_fetch) == nil
      assert Map.get(stream.api_versions, :offset_commit) == nil
    end
  end

  describe "Enumerable protocol" do
    test "count returns error" do
      stream = %Stream{}

      assert Enumerable.count(stream) == {:error, Enumerable.KafkaEx.Consumer.Stream}
    end

    test "member? returns error" do
      stream = %Stream{}

      assert Enumerable.member?(stream, :any) == {:error, Enumerable.KafkaEx.Consumer.Stream}
    end

    test "slice returns error" do
      stream = %Stream{}

      assert Enumerable.slice(stream) == {:error, Enumerable.KafkaEx.Consumer.Stream}
    end
  end

  # ---------------------------------------------------------------------------
  # Error-path unit tests (issue #357) — use a MockClient GenServer to inject
  # canned fetch responses. Mirrors the MockClient pattern at
  # test/kafka_ex/api/kafka_ex_api_produce_test.exs:9
  # ---------------------------------------------------------------------------

  defmodule MockClient do
    @moduledoc false
    use GenServer

    def start_link(responses) when is_list(responses) do
      GenServer.start_link(__MODULE__, %{responses: responses, commits: []})
    end

    def commits(pid), do: GenServer.call(pid, :get_commits)

    @impl true
    def init(state), do: {:ok, state}

    @impl true
    def handle_call({:fetch, _topic, _partition, _offset, _opts}, _from, state) do
      case state.responses do
        [response | rest] ->
          {:reply, response, %{state | responses: rest}}

        [] ->
          {:reply, {:error, :no_more_responses}, state}
      end
    end

    def handle_call({:offset_commit, _group, _commits, _opts}, _from, state) do
      {:reply, {:ok, []}, %{state | commits: [DateTime.utc_now() | state.commits]}}
    end

    def handle_call(:get_commits, _from, state) do
      {:reply, state.commits, state}
    end
  end

  describe "stream error handling (#357)" do
    alias KafkaEx.Consumer.Stream, as: ConsumerStream

    @tag timeout: 5_000
    test "halts on fetch error with no_wait_at_logend: false (no silent spin)" do
      {:ok, mock} = MockClient.start_link([{:error, :timeout}])

      stream = %ConsumerStream{
        client: mock,
        topic: "t",
        partition: 0,
        offset: 0,
        no_wait_at_logend: false,
        fetch_options: [],
        api_versions: %{}
      }

      # Pre-fix: this hangs forever (silent spin on the wildcard fallback).
      # Post-fix: halts cleanly via the explicit error clause.
      {list, log} = ExUnit.CaptureLog.with_log(fn -> Enum.to_list(stream) end)

      assert list == []
      assert log =~ "Stream halting after fetch error"
      assert log =~ ":timeout"
    end

    test "halts on fetch error with no_wait_at_logend: true (pins existing behavior)" do
      {:ok, mock} = MockClient.start_link([{:error, :timeout}])

      stream = %ConsumerStream{
        client: mock,
        topic: "t",
        partition: 0,
        offset: 0,
        no_wait_at_logend: true,
        fetch_options: [],
        api_versions: %{}
      }

      # Pre-fix: the wildcard %{} clause at stream.ex:81-83 catches any error
      # response and halts. This pins that accidental existing behavior — the
      # explicit error clause Task 2 adds preserves it.
      assert Enum.to_list(stream) == []
    end

    test "does not crash with auto_commit + error response (no KeyError)" do
      {:ok, mock} = MockClient.start_link([{:error, :timeout}])

      stream = %ConsumerStream{
        client: mock,
        topic: "t",
        partition: 0,
        offset: 0,
        consumer_group: "test-group",
        no_wait_at_logend: false,
        fetch_options: [auto_commit: true],
        api_versions: %{}
      }

      # Pre-fix: KeyError on fetch_response.message_set inside maybe_commit_offset.
      # Post-fix: need_commit? returns false; no commit call; halt cleanly.
      ExUnit.CaptureLog.capture_log(fn ->
        assert Enum.to_list(stream) == []
      end)

      # No commit should have been recorded.
      assert MockClient.commits(mock) == []
    end

    test "success path still emits messages and advances offset" do
      success_response =
        {:ok,
         %{
           last_offset: 4,
           records: [
             %{offset: 0, value: "a"},
             %{offset: 1, value: "b"},
             %{offset: 2, value: "c"},
             %{offset: 3, value: "d"},
             %{offset: 4, value: "e"}
           ]
         }}

      # Then an empty response with no_wait_at_logend: true so the stream halts.
      empty_response = {:ok, %{last_offset: 4, records: []}}

      {:ok, mock} = MockClient.start_link([success_response, empty_response])

      stream = %ConsumerStream{
        client: mock,
        topic: "t",
        partition: 0,
        offset: 0,
        no_wait_at_logend: true,
        fetch_options: [],
        api_versions: %{}
      }

      values =
        stream
        |> Enum.to_list()
        |> Enum.map(& &1.value)

      assert values == ["a", "b", "c", "d", "e"]
    end
  end
end
