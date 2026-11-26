defmodule KafkaEx.Client.OffsetCommitRetryTest do
  @moduledoc """
  Regression for PR-4 / defect H-5: offset_commit must use `commit_retryable?/1` at
  the client retry layer (not the default `always_retryable`), so a rejoin-signal
  error like `:illegal_generation` fails fast (one attempt) instead of burning the
  full client retry budget against a generation the broker has already invalidated.

  Driven through a started client on the fake transport. A failed send never
  reaches the fake, so attempts are counted by the transport's failure log line.
  """
  use KafkaEx.TestSupport.PinCase, async: false

  @moduletag pin_set: :invariant

  setup do
    {:ok, start_pinned(api_versions: %{offset_commit: 0..3}, client: [consumer_group: "g"])}
  end

  test ":illegal_generation on offset_commit is not retried at the client layer", %{
    fake: fake,
    client: client,
    host: host
  } do
    FakeTransport.fail_send(fake, :offset_commit, 0, :illegal_generation)

    {_reply, log} = with_log(fn -> commit(client) end)

    # commit_retryable?(:illegal_generation) is false -> exactly one wire attempt.
    # Pre-fix (always_retryable) sent it @retry_count (3) times.
    assert failed_sends(log, host, :illegal_generation) == 1
  end

  test ":unknown_topic_id refreshes metadata and returns instead of resending the stale request",
       %{fake: fake, client: client, host: host} do
    # The stale topic id is part of the built request, so resending `ctx.request`
    # can never succeed. The client must refresh metadata and return, so the
    # caller's retry (GenConsumer commit_with_retry) rebuilds it with fresh ids.
    FakeTransport.fail_send(fake, :offset_commit, 0, :unknown_topic_id)
    FakeTransport.fail_send(fake, :metadata, 0, :request_timed_out)

    {reply, log} = with_log(fn -> commit(client) end)

    assert {:error, _} = reply
    # Exactly one OffsetCommit. Before the fix it was resent
    # @retry_count (3) times with the same stale id.
    assert failed_sends(log, host, :unknown_topic_id) == 1
    # ...and metadata was refreshed so the next attempt sees new ids.
    assert failed_sends(log, host, :request_timed_out) >= 1
  end

  defp commit(client) do
    Client.request(client, {:offset_commit, "g", [{"t", [%{partition_num: 0, offset: 5}]}], []}, 5_000)
  end
end
