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

    def handle_call({:offset_commit, _group, topic_partitions, _opts}, _from, state) do
      offsets = for {_topic, partitions} <- topic_partitions, %{offset: o} <- partitions, do: o
      {:reply, {:ok, []}, %{state | commits: state.commits ++ offsets}}
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

  describe "auto-commit offset accuracy (deferred-by-one-batch)" do
    alias KafkaEx.Consumer.Stream, as: ConsumerStream

    defp batch(offsets),
      do: {:ok, %{last_offset: Enum.max(offsets), records: for(o <- offsets, do: %{offset: o, value: "v"})}}

    defp empty(last), do: {:ok, %{last_offset: last, records: []}}

    defp auto_commit_stream(mock, opts \\ [auto_commit: true]),
      do: %ConsumerStream{
        client: mock,
        topic: "t",
        partition: 0,
        offset: 100,
        consumer_group: "g",
        no_wait_at_logend: true,
        fetch_options: opts,
        api_versions: %{}
      }

    test "truncated Enum.take never commits past the last delivered batch (regression)" do
      # b1: 100..102, b2: 103..106. take(5) delivers 100,101,102,103,104.
      {:ok, mock} = MockClient.start_link([batch(100..102), batch(103..106)])

      taken = mock |> auto_commit_stream() |> Enum.take(5) |> Enum.map(& &1.offset)

      assert taken == [100, 101, 102, 103, 104]
      # b1 fully delivered → committed; truncated b2 not (pre-fix wrongly committed 106).
      assert MockClient.commits(mock) == [102]
    end

    test "full drain commits every fully-delivered batch's last offset" do
      {:ok, mock} = MockClient.start_link([batch(100..102), batch(103..105), empty(105)])

      values = mock |> auto_commit_stream() |> Enum.to_list() |> Enum.map(& &1.offset)

      assert values == [100, 101, 102, 103, 104, 105]
      assert MockClient.commits(mock) == [102, 105]
    end

    test "auto_commit: false never commits" do
      {:ok, mock} = MockClient.start_link([batch(100..102), empty(102)])

      _ = mock |> auto_commit_stream(auto_commit: false) |> Enum.to_list()

      assert MockClient.commits(mock) == []
    end
  end

  # A mock whose fetch always returns one record (so auto_commit triggers a commit
  # every cycle) and whose offset_commit returns a configurable error.
  defmodule FatalCommitMockClient do
    @moduledoc false
    use GenServer

    def start_link(commit_response) do
      GenServer.start_link(__MODULE__, %{commit_response: commit_response, fetches: 0})
    end

    def fetches(pid), do: GenServer.call(pid, :fetches)

    @impl true
    def init(state), do: {:ok, state}

    @impl true
    def handle_call({:fetch, _topic, _partition, _offset, _opts}, _from, state) do
      response = {:ok, %{last_offset: 0, records: [%{offset: 0, value: "x"}]}}
      {:reply, response, %{state | fetches: state.fetches + 1}}
    end

    def handle_call({:offset_commit, _group, _commits, _opts}, _from, state) do
      {:reply, state.commit_response, state}
    end

    def handle_call(:fetches, _from, state), do: {:reply, state.fetches, state}
  end

  describe "stream offset-commit failure handling (PR-4 / C-4)" do
    alias KafkaEx.Consumer.Stream, as: ConsumerStream

    @tag timeout: 5_000
    test "halts on a fatal offset-commit error instead of silently looping (duplicate processing)" do
      {:ok, mock} = FatalCommitMockClient.start_link({:error, :illegal_generation})

      stream = %ConsumerStream{
        client: mock,
        topic: "t",
        partition: 0,
        offset: 0,
        consumer_group: "g",
        no_wait_at_logend: false,
        fetch_options: [auto_commit: true],
        api_versions: %{}
      }

      {_list, log} = ExUnit.CaptureLog.with_log(fn -> Enum.take(stream, 50) end)

      # post-fix: halts at the first commit (:illegal_generation is non-retryable, so
      # commit_offset returns immediately) -> exactly 1 fetch. pre-fix: swallows the
      # error and keeps fetching the same batch (~50 fetches) -> reprocessing forever.
      assert FatalCommitMockClient.fetches(mock) == 1
      assert log =~ "Stream halting: offset commit failed"
      assert log =~ ":illegal_generation"
    end
  end

  # MockClient that records the offset of every fetch so we can assert the
  # stream advanced past a control-only batch instead of re-fetching it.
  defmodule OffsetRecordingMockClient do
    @moduledoc false
    use GenServer

    def start_link(responses) when is_list(responses) do
      GenServer.start_link(__MODULE__, %{responses: responses, offsets: []})
    end

    def fetch_offsets(pid), do: pid |> GenServer.call(:get_offsets) |> Enum.reverse()

    @impl true
    def init(state), do: {:ok, state}

    @impl true
    def handle_call({:fetch, _topic, _partition, offset, _opts}, _from, state) do
      state = %{state | offsets: [offset | state.offsets]}

      case state.responses do
        [response | rest] -> {:reply, response, %{state | responses: rest}}
        [] -> {:reply, {:ok, %{last_offset: nil, next_offset: nil, records: []}}, state}
      end
    end

    def handle_call(:get_offsets, _from, state) do
      {:reply, state.offsets, state}
    end
  end

  describe "stream control-batch handling" do
    alias KafkaEx.Consumer.Stream, as: ConsumerStream

    test "advances past a control-only fetch instead of re-fetching the same offset" do
      # First fetch: all records filtered out (control batch) but next_offset (raw
      # batch metadata) points to 6. Second fetch: caught up (no batches) so the
      # stream halts under no_wait_at_logend: true.
      control_only = {:ok, %{last_offset: nil, next_offset: 6, records: []}}
      caught_up = {:ok, %{last_offset: nil, next_offset: nil, records: []}}

      {:ok, mock} = OffsetRecordingMockClient.start_link([control_only, caught_up])

      stream = %ConsumerStream{
        client: mock,
        topic: "t",
        partition: 0,
        offset: 0,
        no_wait_at_logend: true,
        fetch_options: [],
        api_versions: %{}
      }

      assert Enum.to_list(stream) == []

      offsets = OffsetRecordingMockClient.fetch_offsets(mock)
      # Pre-fix: stream halts/spins at offset 0 and never fetches 6.
      assert 0 in offsets
      assert 6 in offsets
    end
  end
end
