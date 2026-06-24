defmodule KafkaEx.Consumer.GenConsumerLoadOffsetsTest.NoopConsumer do
  @moduledoc false
  def init(_topic, _partition, _args), do: {:ok, %{}}
end

defmodule KafkaEx.Consumer.GenConsumerLoadOffsetsTest do
  @moduledoc """
  Regression for PR-4 / defect H-6: when fetching the committed offset fails with a
  real error (coordinator/network), `load_offsets` must NOT silently fall back to
  `auto_offset_reset` — that replaces the committed position with :earliest/:latest,
  causing offset loss/skip or mass reprocessing. It must raise (propagate ->
  supervisor restart). A genuine "no committed offset" still resets.

  Driven through the real `handle_info(:timeout, ...)` startup path; no private
  function is exposed.
  """
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog

  alias __MODULE__.NoopConsumer
  alias KafkaEx.Consumer.GenConsumer
  alias KafkaEx.Consumer.GenConsumer.State
  alias KafkaEx.Test.MockClient

  defp base_state(client) do
    %State{
      current_offset: nil,
      last_commit: nil,
      client: client,
      group: "g",
      topic: "t",
      partition: 0,
      auto_offset_reset: :latest,
      api_versions: %{}
    }
  end

  setup do
    # avoid real backoff sleeps in the retry path
    original = Application.get_env(:kafka_ex, :load_offsets_retry_backoff_ms)
    Application.put_env(:kafka_ex, :load_offsets_retry_backoff_ms, 0)

    on_exit(fn ->
      if is_nil(original),
        do: Application.delete_env(:kafka_ex, :load_offsets_retry_backoff_ms),
        else: Application.put_env(:kafka_ex, :load_offsets_retry_backoff_ms, original)
    end)

    :ok
  end

  test "auto_offset_reset defaults to :latest when set by neither opts nor app env" do
    {:ok, mock} = MockClient.start_link(%{})

    original = Application.get_env(:kafka_ex, :auto_offset_reset)
    Application.delete_env(:kafka_ex, :auto_offset_reset)
    on_exit(fn -> unless is_nil(original), do: Application.put_env(:kafka_ex, :auto_offset_reset, original) end)

    {:ok, state, _timeout} = GenConsumer.init({NoopConsumer, "g", "t", 0, [client: mock]})

    assert state.auto_offset_reset == :latest
  end

  test "init raises on an invalid auto_offset_reset value" do
    {:ok, mock} = MockClient.start_link(%{})

    assert_raise ArgumentError, ~r/invalid auto_offset_reset/, fn ->
      GenConsumer.init({NoopConsumer, "g", "t", 0, [client: mock, auto_offset_reset: :bogus]})
    end
  end

  test "resetting for no committed offset logs and emits :offset_reset telemetry" do
    {:ok, mock} =
      MockClient.start_link(%{
        offset_fetch: {:ok, [%{partition_offsets: [%{offset: -1, error_code: :no_error}]}]},
        list_offsets: {:ok, [%{partition_offsets: [%{offset: 42}]}]}
      })

    test_pid = self()
    handler_id = "offset-reset-telemetry-test"

    :telemetry.attach(
      handler_id,
      [:kafka_ex, :consumer, :offset_reset],
      fn _event, measurements, metadata, _ -> send(test_pid, {:offset_reset, measurements, metadata}) end,
      nil
    )

    on_exit(fn -> :telemetry.detach(handler_id) end)

    state = %State{base_state(mock) | auto_offset_reset: :latest}

    log =
      capture_log(fn ->
        assert {:noreply, new_state, 0} = GenConsumer.handle_info(:timeout, state)
        assert new_state.current_offset == 42
      end)

    assert log =~ "No committed offset for t/0"
    assert log =~ "auto_offset_reset=:latest"

    assert_received {:offset_reset, %{offset: 42},
                     %{topic: "t", partition: 0, reset: :latest, reason: :no_committed_offset}}
  end

  test "raises on a fatal fetch error instead of silently resetting to auto_offset_reset" do
    {:ok, mock} = MockClient.start_link(%{offset_fetch: {:error, :group_authorization_failed}})

    assert_raise RuntimeError, ~r/Unable to load committed offsets/, fn ->
      GenConsumer.handle_info(:timeout, base_state(mock))
    end
  end

  test "raises when there is no committed offset and auto_offset_reset is :none" do
    {:ok, mock} =
      MockClient.start_link(%{offset_fetch: {:ok, [%{partition_offsets: [%{offset: -1, error_code: :no_error}]}]}})

    state = %State{base_state(mock) | auto_offset_reset: :none}

    assert_raise RuntimeError, ~r/auto_offset_reset is :none/, fn ->
      GenConsumer.handle_info(:timeout, state)
    end
  end

  test ":unstable_offset_commit (KIP-447) is retried, not raised on the first attempt" do
    {:ok, mock} = MockClient.start_link(%{offset_fetch: {:error, :unstable_offset_commit}})

    # it still raises once retries are exhausted, but only AFTER retrying
    assert_raise RuntimeError, fn -> GenConsumer.handle_info(:timeout, base_state(mock)) end

    # @load_offsets_max_attempts (6) total attempts before raising
    fetch_calls = mock |> MockClient.get_calls() |> Enum.count(&match?({:offset_fetch, _, _, _}, &1))
    assert fetch_calls == 6
  end
end
