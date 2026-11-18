defmodule KafkaEx.Client.CoordinatorRefreshTest do
  @moduledoc """
  Regression for PR-3 / defect C-3: a NOT_COORDINATOR (or COORDINATOR_NOT_AVAILABLE)
  response on a consumer-group request must invalidate the cached coordinator and
  re-run FindCoordinator before retrying — not blind-retry the stale broker.

  Pre-fix, the metadata-refresh classifier only knew leadership errors, so
  `:not_coordinator` retried the same cached coordinator and FindCoordinator was
  never re-issued (the rolling-deploy hang). Coordinator-refresh classification now
  lives in `Retry.coordinator_refresh_error?/1`.

  The two tests cover the two ways `:not_coordinator` reaches the retry loop. The
  first injects it at the network seam, where it becomes an `%Error{}`. The second
  is the DescribeGroups API which reports per-group failures as
  `{:error, [{group_id, error_atom}]}`.

  Each test first caches the coordinator with one successful request; a
  FindCoordinator in the fake's request log after that is a re-discovery.
  A failed send never reaches the fake, so it is counted by its log line.
  """
  use KafkaEx.TestSupport.PinCase, async: false

  @moduletag pin_set: :invariant

  @not_coordinator 16
  @group "test-group"
  @offset_fetch_ok %{
    error_code: 0,
    topics: [%{name: "t", partitions: [%{partition_index: 0, committed_offset: 0, metadata: nil, error_code: 0}]}]
  }

  setup do
    pinned =
      start_pinned(
        api_versions: %{offset_fetch: 0..3, describe_groups: 0..0, consumer_group_heartbeat: 0..1},
        client: [consumer_group: @group]
      )

    {:ok, pinned}
  end

  test ":not_coordinator on a consumer-group request triggers coordinator re-discovery", %{fake: fake, client: client} do
    FakeTransport.respond(fake, :offset_fetch, @offset_fetch_ok)
    mark = cache_coordinator(fake, fn -> offset_fetch(client, []) end)
    FakeTransport.fail_send(fake, :offset_fetch, 0, :not_coordinator)

    with_log(fn -> offset_fetch(client, []) end)

    # pre-fix this never fires: :not_coordinator retried the stale cached coordinator
    assert :find_coordinator in apis_after(fake, mark)
  end

  test "a group-level :not_coordinator in a DescribeGroups response triggers re-discovery", %{
    fake: fake,
    client: client
  } do
    FakeTransport.respond(fake, :describe_groups, &{:frame, describe_groups_v0_response(&1.correlation_id, @group, 0)})
    mark = cache_coordinator(fake, fn -> describe(client) end)

    FakeTransport.respond(
      fake,
      :describe_groups,
      &{:frame, describe_groups_v0_response(&1.correlation_id, @group, @not_coordinator)}
    )

    with_log(fn -> describe(client) end)

    # pre-fix this never fires: the group-level atom arrived as :unknown
    assert :find_coordinator in apis_after(fake, mark)
  end

  describe "KIP-848 ConsumerGroupHeartbeat on a connection failure" do
    setup %{fake: fake, client: client} do
      FakeTransport.respond(fake, :consumer_group_heartbeat, &{:frame, consumer_group_heartbeat_frame(&1, "m", 5)})
      {:ok, mark: cache_coordinator(fake, fn -> heartbeat(client, member_id: "m", member_epoch: 5) end)}
    end

    # Nothing was sent: the client retries, on a re-discovered coordinator.
    for reason <- [:no_broker, :not_connected] do
      test "#{reason} re-discovers the coordinator before the next attempt", %{fake: fake, client: client, mark: mark} do
        FakeTransport.fail_send(fake, :consumer_group_heartbeat, 0, unquote(reason))

        with_log(fn -> heartbeat(client, member_id: "m", member_epoch: 5) end)

        # pre-fix the client retried the cached coordinator only
        assert :find_coordinator in apis_after(fake, mark)
      end
    end

    # The broker may have processed it (and bumped the epoch): an incremental
    # heartbeat resent unchanged would be fenced, so it goes back to the
    # manager, which resends it in full.
    for reason <- [:timeout, :closed, :econnreset, :transport_error, :parse_error] do
      test "#{reason} is send-once: returned to the caller, not resent", %{
        fake: fake,
        client: client,
        host: host,
        mark: mark
      } do
        reason = unquote(reason)
        FakeTransport.fail_send(fake, :consumer_group_heartbeat, 0, reason)

        {result, log} = with_log(fn -> heartbeat(client, member_id: "m", member_epoch: 5) end)

        assert {:error, %KafkaEx.Client.Error{error: ^reason}} = result
        assert failed_sends(log, host, reason) == 1
        refute :find_coordinator in apis_after(fake, mark)
        refute :consumer_group_heartbeat in apis_after(fake, mark)
      end
    end

    # A join (0) and a leave (-1 dynamic, -2 static) are not incremental: the
    # broker accepts them again, so the client keeps retrying a lost
    # connection (the leave has no other retry).
    for epoch <- [0, -1, -2] do
      test "a heartbeat with epoch #{epoch} is still resent by the client after :closed", %{
        fake: fake,
        client: client,
        host: host,
        mark: mark
      } do
        epoch = unquote(epoch)
        FakeTransport.fail_send(fake, :consumer_group_heartbeat, 0, :closed)

        opts = [member_id: "m", member_epoch: epoch] ++ if(epoch == -2, do: [instance_id: "i"], else: [])
        {_result, log} = with_log(fn -> heartbeat(client, opts) end)

        assert failed_sends(log, host, :closed) >= 2
        assert :find_coordinator in apis_after(fake, mark)
      end
    end

    test "a send-once failure forgets the coordinator, so the next heartbeat re-discovers it", %{
      fake: fake,
      client: client,
      mark: mark
    } do
      FakeTransport.fail_send(fake, :consumer_group_heartbeat, 0, :closed)

      {result, _log} = with_log(fn -> heartbeat(client, member_id: "m", member_epoch: 5) end)

      assert {:error, _} = result
      refute :find_coordinator in apis_after(fake, mark)

      FakeTransport.release(fake, :consumer_group_heartbeat)
      heartbeat(client, member_id: "m", member_epoch: 5)

      assert :find_coordinator in apis_after(fake, mark)
    end

    test "a broker error answer (not a connection failure) keeps the cached coordinator", %{
      fake: fake,
      client: client,
      mark: mark
    } do
      FakeTransport.fail_send(fake, :consumer_group_heartbeat, 0, :request_timed_out)

      {result, _log} = with_log(fn -> heartbeat(client, member_id: "m", member_epoch: 5) end)

      assert {:error, _} = result
      FakeTransport.release(fake, :consumer_group_heartbeat)
      assert {:ok, _} = heartbeat(client, member_id: "m", member_epoch: 5)

      refute :find_coordinator in apis_after(fake, mark)
    end

    test "a classic request keeps its behaviour: a :closed is retried on the cached coordinator", %{
      fake: fake,
      client: client
    } do
      FakeTransport.respond(fake, :offset_fetch, @offset_fetch_ok)
      mark = cache_coordinator(fake, fn -> offset_fetch(client, api_version: 3) end)
      FakeTransport.fail_send(fake, :offset_fetch, 0, :closed)

      {result, _log} = with_log(fn -> offset_fetch(client, api_version: 3) end)

      assert {:error, _} = result
      FakeTransport.release(fake, :offset_fetch)
      assert {:ok, _} = offset_fetch(client, api_version: 3)

      refute :find_coordinator in apis_after(fake, mark)
    end
  end

  defp cache_coordinator(fake, call) do
    assert {:ok, _} = call.()
    length(FakeTransport.requests(fake))
  end

  defp offset_fetch(client, opts),
    do: GenServer.call(client, {:offset_fetch, @group, [{"t", [%{partition_num: 0}]}], opts})

  defp describe(client), do: GenServer.call(client, {:describe_groups, [@group], [api_version: 0]})
  defp heartbeat(client, opts), do: GenServer.call(client, {:consumer_group_heartbeat, @group, opts})

  # correlation_id, one group: error_code, group_id, empty state/protocol_type/protocol,
  # no members.
  defp describe_groups_v0_response(correlation_id, group_id, error_code) do
    <<correlation_id::32-signed, 1::32-signed, error_code::16-signed, byte_size(group_id)::16-signed>> <>
      group_id <> <<0::16-signed, 0::16-signed, 0::16-signed, 0::32-signed>>
  end
end
