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

  test "raises on a fatal fetch error instead of silently resetting to auto_offset_reset" do
    {:ok, mock} = MockClient.start_link(%{offset_fetch: {:error, :group_authorization_failed}})

    assert_raise RuntimeError, ~r/Unable to load committed offsets/, fn ->
      GenConsumer.handle_info(:timeout, base_state(mock))
    end
  end

  test ":unstable_offset_commit (KIP-447) is retried, not raised on the first attempt" do
    {:ok, mock} = MockClient.start_link(%{offset_fetch: {:error, :unstable_offset_commit}})

    # it still raises once retries are exhausted, but only AFTER retrying
    assert_raise RuntimeError, fn -> GenConsumer.handle_info(:timeout, base_state(mock)) end

    # 1 initial attempt + @load_offsets_max_retries (5) retries = 6
    fetch_calls = mock |> MockClient.get_calls() |> Enum.count(&match?({:offset_fetch, _, _, _}, &1))
    assert fetch_calls == 6
  end
end
