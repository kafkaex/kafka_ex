defmodule KafkaEx.Client.TransportErrorTest do
  @moduledoc """
  Regression for PR-2 / defect C-1: a transport-level failure (recv-timeout,
  socket closed, ...) must surface its REAL error atom through the client retry
  loop, not be collapsed to `:unknown`.

  Pre-fix, the client retry loop caught the network result with a
  `{_, _state_out}` wildcard and substituted `Error.build(:unknown, %{})`,
  destroying the real reason before any `retryable?` predicate or the
  consumer-group manager could see it. That relabeling is what turned a
  recv-timeout to the coordinator into a `KafkaEx.SyncGroupError ... :unknown`
  crash in production.
  """
  use KafkaEx.TestSupport.PinCase, async: false

  alias KafkaEx.Client.Error

  @moduletag pin_set: :invariant

  setup do
    {:ok, start_pinned(api_versions: %{offset_fetch: 0..3}, client: [consumer_group: "test-group"])}
  end

  test "recv-timeout surfaces the real :timeout atom, not :unknown", %{fake: fake, client: client} do
    FakeTransport.fail_send(fake, :offset_fetch, 0, :timeout)

    {result, log} =
      with_log(fn ->
        Client.request(client, {:offset_fetch, "test-group", [{"t", [%{partition_num: 0}]}], []}, 5_000)
      end)

    assert {:error, %Error{error: :timeout}} = result
    assert log =~ "failed with error :timeout"
    refute log =~ "failed with error :unknown"
  end
end
