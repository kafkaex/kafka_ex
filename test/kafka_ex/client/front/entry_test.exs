defmodule KafkaEx.Client.Front.EntryTest do
  use ExUnit.Case, async: true

  alias KafkaEx.Client.Error
  alias KafkaEx.Client.Front.Entry
  alias KafkaEx.TestSupport.FrontPolicies

  @b1 FrontPolicies.conn(1, :data)
  @b2 FrontPolicies.conn(2, :data)
  @m1 FrontPolicies.conn(1, :metadata)
  @m2 FrontPolicies.conn(2, :metadata)
  @r3 FrontPolicies.conn(3, {:fetch, "t", 0})

  defp start(class, deadline \\ 5_000) do
    {entry, [{:resolve, 1, _}]} =
      Entry.transition(Entry.new(1, FrontPolicies.policy(class, 0), deadline), {0, 0.5, :start})

    entry
  end

  defp step(entry, now, body), do: Entry.transition(entry, {now, 0.5, body})
  defp hit(conn, epoch \\ 0), do: {:routed, {:node, conn, %{node: 1, replica?: false, epoch: epoch}}}

  defp retried_in_flight do
    {entry, _} = step(start(:produce, 100), 0, hit(@b1))
    {entry, _} = step(entry, 5, {:done, 1, {:error, :not_leader_for_partition}})
    {entry, _} = step(entry, 7, {:swapped, %{"t" => 1}})
    {entry, [{:dispatch, %{ref: {1, 2}}}]} = step(entry, 7, hit(@b2, 1))
    entry
  end

  test "a hit dispatches attempt 1 under a fresh ref" do
    {entry, [{:dispatch, d}]} = step(start(:fetch), 0, hit(@b1))
    assert %{ref: {1, 1}, conn: @b1, attempt: 1, wait?: true, woken?: false} = d
    assert entry.phase == :in_flight
  end

  test "a reply for a superseded ref is dropped" do
    {entry, _} = step(start(:fetch), 0, hit(@b1))
    assert step(entry, 5, {:done, 7, {:ok, :late}}) == {entry, []}
  end

  test "a refusal parks on the connection at no cost, and :connected wakes it" do
    {entry, _} = step(start(:produce), 0, hit(@b1))
    {entry, [{:connect, @b1}]} = step(entry, 5, {:done, 1, {:error, :not_connected}})
    assert {entry.phase, entry.spent} == {:awaiting_connection, 0}
    assert step(entry, 6, {:connected, @b2}) == {entry, []}
    {entry, [{:resolve, 1, _}]} = step(entry, 7, {:connected, @b1})
    {_entry, [{:dispatch, d}]} = step(entry, 7, hit(@b1))
    assert %{attempt: 1, woken?: true, ref: {1, 2}} = d
  end

  test "a leadership error refreshes, parks, and wakes only on a newer epoch of its key" do
    {entry, _} = step(start(:produce), 0, hit(@b1))
    {entry, effects} = step(entry, 5, {:done, 1, {:error, :not_leader_for_partition}})

    assert [{:classified, 1, :not_leader_for_partition, :retry}, {:store, {:refresh_metadata, _}}, {:resolve, 1, _}] =
             effects

    assert step(entry, 5, hit(@b1, 0)) == {entry, []}
    assert step(entry, 6, {:swapped, %{{:coordinator, "g"} => 9}}) == {entry, []}
    {entry, [{:resolve, 1, _}]} = step(entry, 7, {:swapped, %{"t" => 1}})
    {_entry, [{:dispatch, %{attempt: 2}}]} = step(entry, 7, hit(@b2, 1))
  end

  test "a retried timeout waits for its own backoff tick" do
    {entry, _} = step(start(:fetch), 0, hit(@b1))

    {entry, [{:classified, 1, :timeout, :retry}, {:timer, {:backoff, 1, 1}, at}]} =
      step(entry, 10, {:done, 1, {:error, :timeout}})

    assert at - 10 >= 80
    assert step(entry, at, {:tick, 0}) == {entry, []}
    assert {_entry, [{:resolve, 1, _}]} = step(entry, at, {:tick, 1})
  end

  test "the deadline ends an entry with :timeout in flight and :no_broker while parked" do
    {entry, _} = step(start(:fetch, 100), 0, hit(@b1))
    assert {_, [{:end, 1, {:reply, {:error, :timeout}}}]} = step(entry, 100, :deadline)
    {entry, _} = step(entry, 5, {:done, 1, {:error, :not_connected}})
    assert {_, [{:end, 1, {:reply, {:error, :no_broker}}}]} = step(entry, 100, :deadline)
  end

  test "a late success still replies the success" do
    {entry, _} = step(start(:produce, 100), 0, hit(@b1))
    assert {_, [{:end, 1, {:reply, {:ok, :answer}}}]} = step(entry, 150, {:done, 1, {:ok, :answer}})
  end

  test "a caller that went down ends the entry without a reply" do
    {entry, _} = step(start(:fetch), 0, hit(@b1))
    assert {%{phase: :ended}, [{:end, 1, :caller_down}]} = step(entry, 5, :caller_down)
  end

  test "a first_available walk moves past refusals and written failures, then gives up with :broker_failed" do
    {entry, [{:dispatch, %{conn: @m1, wait?: false}}]} =
      step(start(:send_any), 0, {:routed, {:candidates, [@m1, @m2], 0}})

    {entry, [{:dispatch, %{conn: @m2, attempt: 1}}]} = step(entry, 1, {:done, 1, {:error, :not_connected}})
    assert {_, [{:end, 1, {:reply, {:error, :broker_failed}}}]} = step(entry, 2, {:done, 2, {:error, :closed}})
  end

  test "a walk where no candidate was written parks on all of them" do
    {entry, _} = step(start(:send_any), 0, {:routed, {:candidates, [@m1], 0}})
    {entry, [{:connect, @m1}]} = step(entry, 1, {:done, 1, {:error, :not_connected}})
    assert {entry.phase, entry.parked_on} == {:awaiting_connection, [@m1]}
  end

  test "an error from a preferred replica clears the hint and goes to the leader" do
    replica = {:routed, {:node, FrontPolicies.conn(3, {:fetch, "t", 0}), %{node: 3, replica?: true, epoch: 0}}}
    {entry, _} = step(start(:fetch), 0, replica)
    {entry, effects} = step(entry, 5, {:done, 1, {:error, :offset_out_of_range}})

    assert [{:classified, 1, :offset_out_of_range, :replica}, {:clear_hint, {"t", 0}, 3}, {:timer, _, at}] =
             effects

    assert {_, [{:resolve, 1, %{leader_only?: true}}]} = step(entry, at, {:tick, 1})
  end

  test "acks 0 refused by a reconnecting connection fails at once" do
    {entry, _} = step(start(:acks0), 0, hit(@b1))
    assert {_, [{:end, 1, {:reply, {:error, :no_broker}}}]} = step(entry, 1, {:done, 1, {:error, :not_connected}})
  end

  test "a store restart re-issues the trigger of a parked entry" do
    {entry, [{:store, trigger}, {:resolve, 1, _}]} = step(start(:produce), 0, {:routed, {:miss, 0}})
    assert step(entry, 5, :store_restarted) == {entry, [{:store, trigger}]}
  end

  test "a refused KIP-848 heartbeat forgets the coordinator, asks for a new one and parks" do
    coordinator = FrontPolicies.conn(1, :coordinator)
    {entry, _} = step(start(:kip848_member), 0, hit(coordinator))
    {entry, effects} = step(entry, 5, {:done, 1, {:error, :not_connected}})
    assert [{:store, {:drop_coordinator, "g"}}, {:store, {:rediscover_coordinator, "g"}}, {:resolve, 1, _}] = effects
    assert {entry.phase, entry.spent} == {:awaiting_coordinator, 0}
  end

  test "a park after a written error ends at the deadline with that error" do
    {entry, _} = step(start(:produce, 100), 0, hit(@b1))
    {entry, _} = step(entry, 5, {:done, 1, {:error, :not_leader_for_partition}})
    assert entry.phase == :awaiting_metadata
    assert {_, [{:end, 1, {:reply, {:error, :not_leader_for_partition}}}]} = step(entry, 100, :deadline)
  end

  test "a park that never wrote ends at the deadline with :no_broker" do
    {entry, _} = step(start(:produce, 100), 0, {:routed, {:miss, 0}})
    assert entry.phase == :awaiting_metadata
    assert {_, [{:end, 1, {:reply, {:error, :no_broker}}}]} = step(entry, 100, :deadline)
  end

  test "a backoff after a written transport error ends at the deadline with that error" do
    {entry, _} = step(start(:fetch, 300), 0, hit(@b1))
    {entry, [{:classified, 1, :closed, :retry}, {:timer, _, _}]} = step(entry, 10, {:done, 1, {:error, :closed}})
    assert {_, [{:end, 1, {:reply, {:error, :closed}}}]} = step(entry, 300, :deadline)
  end

  test "a written crash replaces an earlier error as the one the deadline reports" do
    {entry, _} = step(start(:fetch, 1_000), 0, hit(@b1))
    {entry, [_, {:timer, {:backoff, 1, 1}, at}]} = step(entry, 10, {:done, 1, {:error, :closed}})
    {entry, _} = step(entry, at, {:tick, 1})
    {entry, [{:dispatch, %{ref: {1, 2}}}]} = step(entry, at, hit(@b1))
    {entry, [{:classified, 1, :timeout, :retry}, {:timer, _, _}]} = step(entry, at + 5, {:conn_down, @b1, true})
    assert {_, [{:end, 1, {:reply, {:error, :timeout}}}]} = step(entry, 1_000, :deadline)
  end

  test "a retry in flight at the deadline still ends with :timeout" do
    assert {_, [{:end, 1, {:reply, {:error, :timeout}}}]} = step(retried_in_flight(), 100, :deadline)
  end

  test "an unwritten crash with no time to back off replies the last written error" do
    assert {_, [{:end, 1, {:reply, {:error, :not_leader_for_partition}}}]} =
             step(retried_in_flight(), 50, {:conn_down, @b2, false})
  end

  test "a refusal at the deadline replies the last written error, or :no_broker if nothing was written" do
    assert {_, [{:end, 1, {:reply, {:error, :not_leader_for_partition}}}]} =
             step(retried_in_flight(), 100, {:done, 2, {:error, :not_connected}})

    {entry, _} = step(start(:produce, 100), 0, hit(@b1))
    assert {_, [{:end, 1, {:reply, {:error, :no_broker}}}]} = step(entry, 100, {:done, 1, {:error, :not_connected}})
  end

  test "a walk exhausted by a refusal at the deadline after a written failure replies :broker_failed" do
    {entry, _} = step(start(:send_any, 100), 0, {:routed, {:candidates, [@m1, @m2], 0}})
    {entry, [{:dispatch, %{conn: @m2}}]} = step(entry, 10, {:done, 1, {:error, :closed}})
    assert {_, [{:end, 1, {:reply, {:error, :broker_failed}}}]} = step(entry, 100, {:done, 2, {:error, :not_connected}})
  end

  test "a walk moves on after any Connection failure but ends on a broker answer" do
    {walk, _} = step(start(:find_any), 0, {:routed, {:candidates, [@m1, @m2], 0}})
    assert {_, [{:dispatch, %{conn: @m2}}]} = step(walk, 5, {:done, 1, {:error, :etimedout}})

    for {reason, error} <- [
          {:parse_error, :parse_error},
          {:coordinator_not_available, :coordinator_not_available},
          {%Error{error: :coordinator_not_available, metadata: %{}}, :coordinator_not_available},
          {%Error{error: :empty_response, metadata: %{}}, :empty_response}
        ] do
      {entry, [{:classified, 1, ^error, _branch} | _]} = step(walk, 5, {:done, 1, {:error, reason}})
      assert {entry.spent, entry.walk} == {1, nil}
    end
  end

  test "an encode failure ends the request with :timeout, no retry, no cost and no actions" do
    for class <- [:fetch, :kip848_member, :produce, :acks0], now <- [5, 100] do
      {entry, _} = step(start(class, 100), 0, hit(@b1))

      assert {%{phase: :ended, spent: 0, wire: nil},
              [{:classified, 1, :timeout, :not_retryable}, {:end, 1, {:reply, {:error, :timeout}}}]} =
               step(entry, now, {:encode_failed, 1})
    end
  end

  test "an encode failure ends a walk with candidates left" do
    {entry, _} = step(start(:find_any), 0, {:routed, {:candidates, [@m1, @m2], 0}})
    assert {%{phase: :ended}, [_, {:end, 1, {:reply, {:error, :timeout}}}]} = step(entry, 5, {:encode_failed, 1})
  end

  test "an encode failure for a superseded dispatch is dropped" do
    entry = retried_in_flight()
    assert entry.wire == {2, @b2, false}
    assert step(entry, 8, {:encode_failed, 1}) == {entry, []}
  end

  test "a walk's written transport error at the deadline replies that error" do
    {entry, _} = step(start(:send_any, 100), 0, {:routed, {:candidates, [@m1, @m2], 0}})
    assert {_, [{:end, 1, {:reply, {:error, :closed}}}]} = step(entry, 100, {:done, 1, {:error, :closed}})
  end

  test "an event that arrives at or after the deadline ends the entry instead of acting on it" do
    {entry, _} = step(start(:produce, 100), 0, hit(@b1))
    {entry, [{:connect, @b1}]} = step(entry, 5, {:done, 1, {:error, :not_connected}})
    assert {_, [{:end, 1, {:reply, {:error, :no_broker}}}]} = step(entry, 100, {:connected, @b1})

    {entry, _} = step(start(:fetch, 300), 0, hit(@b1))
    {entry, [_, {:timer, {:backoff, 1, 1}, _}]} = step(entry, 10, {:done, 1, {:error, :closed}})
    assert {_, [{:end, 1, {:reply, {:error, :closed}}}]} = step(entry, 300, {:tick, 1})
  end

  test "a non-atom Connection reason is retried as :transport_error and replied as it came" do
    reason = {:tls_alert, {:bad_record_mac, ~c"x"}}
    {entry, _} = step(start(:fetch, 300), 0, hit(@b1))

    {entry, [{:classified, 1, :transport_error, :retry}, {:timer, _, _}]} =
      step(entry, 10, {:done, 1, {:error, reason}})

    assert {_, [{:end, 1, {:reply, {:error, ^reason}}}]} = step(entry, 300, :deadline)
  end

  defp from_replica(deadline, now \\ 5, reason \\ %Error{error: :offset_out_of_range, metadata: %{}}) do
    {entry, _} = step(start(:fetch, deadline), 0, {:routed, {:node, @r3, %{node: 3, replica?: true, epoch: 0}}})
    step(entry, now, {:done, 1, {:error, reason}})
  end

  test "a replica's error is not replied when the backoff to the leader outlives the deadline" do
    {entry, [_, _, {:timer, _, _}]} = from_replica(200)
    assert {_, [{:end, 1, {:reply, {:error, :timeout}}}]} = step(entry, 200, {:tick, 1})
  end

  test "a replica's error is not replied when the leader is unreachable at the deadline" do
    {entry, [_, _, {:timer, _, at}]} = from_replica(200)
    {entry, [{:resolve, 1, %{leader_only?: true}}]} = step(entry, at, {:tick, 1})
    {entry, [{:dispatch, %{ref: {1, 2}}}]} = step(entry, at, hit(@b1))

    assert {_, [{:end, 1, {:reply, {:error, :no_broker}}}]} = step(entry, 200, {:done, 2, {:error, :not_connected}})

    {entry, [{:connect, @b1}]} = step(entry, at + 1, {:done, 2, {:error, :not_connected}})
    assert {_, [{:end, 1, {:reply, {:error, :no_broker}}}]} = step(entry, 200, :deadline)
  end

  test "a replica's error is not replied when no backoff fits or it arrives at the deadline" do
    assert {_, [_, _, {:end, 1, {:reply, {:error, :timeout}}}]} = from_replica(104)
    assert {_, [_, _, {:end, 1, {:reply, {:error, :timeout}}}]} = from_replica(200, 200)
  end

  test "a replica's stale topic id goes to the leader instead of being replied" do
    assert {%{phase: :backoff},
            [{:classified, 1, :unknown_topic_id, :replica}, {:clear_hint, _, 3}, {:store, _}, {:timer, _, _}]} =
             from_replica(1_000, 5, :unknown_topic_id)
  end

  test "the leader's own written error is what the deadline reports after a replica fails" do
    {entry, _} = step(start(:fetch, 1_000), 0, hit(@b1))
    {entry, [_, {:timer, _, at}]} = step(entry, 10, {:done, 1, {:error, :closed}})
    {entry, _} = step(entry, at, {:tick, 1})

    {entry, [{:dispatch, %{ref: {1, 2}}}]} =
      step(entry, at, {:routed, {:node, @r3, %{node: 3, replica?: true, epoch: 0}}})

    {crashed, [{:classified, 1, :timeout, :replica} | _]} = step(entry, at + 5, {:conn_down, @r3, true})

    {answered, [{:classified, 1, :offset_out_of_range, :replica} | _]} =
      step(entry, at + 5, {:done, 2, {:error, :offset_out_of_range}})

    for entry <- [crashed, answered],
        do: assert({_, [{:end, 1, {:reply, {:error, :closed}}}]} = step(entry, 1_000, :deadline))
  end
end
