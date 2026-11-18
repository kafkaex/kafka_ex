defmodule KafkaEx.Client.Front.DecisionTest do
  use ExUnit.Case, async: true

  alias KafkaEx.Client.Error
  alias KafkaEx.Client.Front.Decision
  alias KafkaEx.Client.Front.Policy
  alias KafkaEx.Client.NodeSelector
  alias KafkaEx.Client.RequestContext
  alias KafkaEx.Support.Retry
  alias KafkaEx.TestSupport.FrontPolicies

  @refresh {:refresh_metadata, %{topics: [], allow_auto_topic_creation: false, api_version: nil}}
  @rediscover [{:drop_coordinator, "g"}, {:rediscover_coordinator, "g"}]

  test "a coordinator request returns an error in its send-once set at once" do
    assert Decision.written(FrontPolicies.policy(:heartbeat, 0), 1, :timeout) == {:reply, :send_once, []}
    assert Decision.written(FrontPolicies.policy(:join, 0), 1, :timeout) == {:reply, :send_once, []}

    assert Decision.written(FrontPolicies.policy(:kip848_member, 0), 1, :closed) ==
             {:reply, :send_once, [{:drop_coordinator, "g"}]}
  end

  test "produce never resends a timeout and parks on a leadership error while budget remains" do
    assert Decision.written(FrontPolicies.policy(:produce, 0), 1, :timeout) == {:reply, :not_retryable, []}

    assert Decision.written(FrontPolicies.policy(:produce, 0), 1, :not_leader_for_partition) ==
             {:park_metadata, :retry, [@refresh]}

    assert Decision.written(FrontPolicies.policy(:produce, 0), 3, :not_leader_for_partition) ==
             {:reply, :exhausted, [@refresh]}
  end

  test "a coordinator error re-discovers, and parks only while budget remains" do
    assert Decision.written(FrontPolicies.policy(:heartbeat, 0), 1, :not_coordinator) ==
             {:park_coordinator, :retry, @rediscover}

    assert Decision.written(FrontPolicies.policy(:join, 0), 1, :not_coordinator) == {:reply, :exhausted, @rediscover}
  end

  test "a leadership error on a request not routed by partition backs off instead of parking" do
    assert Decision.written(FrontPolicies.policy(:offset_fetch, 0), 1, :not_leader_for_partition) ==
             {:backoff, :retry, [@refresh]}
  end

  test "a stale topic id refreshes the metadata and replies" do
    assert Decision.written(FrontPolicies.policy(:fetch, 0), 1, :unknown_topic_id) ==
             {:reply, :unknown_topic_id, [@refresh]}
  end

  test "send_request/4 and acks 0 never resend a written request" do
    assert Decision.written(FrontPolicies.policy(:send_node, 0), 1, :closed) == {:reply, :send_once, []}
    assert Decision.written(FrontPolicies.policy(:acks0, 0), 1, :timeout) == {:reply, :send_once, []}
  end

  test "a refusal costs nothing and chooses where to wait" do
    assert Decision.refused(FrontPolicies.policy(:acks0, 0), false) == {:reply, []}
    assert Decision.refused(FrontPolicies.policy(:send_any, 0), false) == {:next_candidate, []}
    assert Decision.refused(FrontPolicies.policy(:fetch, 0), true) == {:leader, []}

    assert Decision.refused(FrontPolicies.policy(:kip848_member, 0), false) ==
             {:park_coordinator, [{:drop_coordinator, "g"}]}

    assert Decision.refused(FrontPolicies.policy(:produce, 0), false) == {:park_connection, []}
  end

  test "a KIP-848 join retries a dropped connection that a member sends once" do
    assert Decision.written(FrontPolicies.policy(:kip848_join, 0), 1, :closed) ==
             {:backoff, :retry, [{:drop_coordinator, "g"}]}
  end

  test "sync, commit and a first_available lookup follow their own predicates" do
    assert Decision.written(FrontPolicies.policy(:sync, 0), 1, :rebalance_in_progress) == {:reply, :not_retryable, []}
    assert Decision.written(FrontPolicies.policy(:sync, 0), 1, :not_coordinator) == {:reply, :exhausted, @rediscover}
    assert Decision.written(FrontPolicies.policy(:commit, 0), 1, :rebalance_in_progress) == {:backoff, :retry, []}
    assert Decision.written(FrontPolicies.policy(:commit, 0), 3, :rebalance_in_progress) == {:reply, :exhausted, []}

    assert Decision.written(FrontPolicies.policy(:find_any, 0), 1, :not_leader_for_partition) ==
             {:backoff, :retry, [@refresh]}

    assert Decision.refused(FrontPolicies.policy(:find_any, 0), false) == {:next_candidate, []}
  end

  test "any Connection failure but :parse_error is a transport failure, and a broker answer is not" do
    for reason <- [:timeout, :closed, :econnreset, :transport_error, :etimedout, :epipe, {:tls_alert, {:bad, ~c"x"}}],
        do: assert(Decision.transport_failure?(reason), inspect(reason))

    for reason <- [
          :parse_error,
          :coordinator_not_available,
          :not_leader_for_partition,
          %Error{error: :empty_response, metadata: %{}},
          {"g", :not_coordinator}
        ],
        do: refute(Decision.transport_failure?(reason), inspect(reason))
  end

  test "a non-atom Connection reason is classified as :transport_error" do
    reason = {:tls_alert, {:bad_record_mac, ~c"x"}}
    assert Decision.error_atom(reason) == :transport_error
    assert Decision.error_atom({:timeout, "rest"}) == :transport_error

    assert Decision.written(FrontPolicies.policy(:fetch, 0), 1, Decision.error_atom(reason)) ==
             {:backoff, :retry, []}

    assert Decision.written(FrontPolicies.policy(:kip848_member, 0), 1, Decision.error_atom(reason)) ==
             {:reply, :send_once, [{:drop_coordinator, "g"}]}
  end

  test "send_request/4 and acks 0 policies retry nothing" do
    for class <- [:acks0, :send_node, :send_any],
        error <- [:timeout, :closed, :not_leader_for_partition],
        do: refute(FrontPolicies.policy(class, 0).retryable?.(error))
  end

  test "a gated policy that would retry :timeout is refused" do
    ctx = %RequestContext{
      request: :model,
      parser_fn: & &1,
      node_selector: NodeSelector.topic_partition("t", 0),
      retryable?: &Retry.data_plane_retryable?/1
    }

    assert_raise ArgumentError, fn -> Policy.from_context(ctx, max_attempts: 3, gate: {"t", 0}) end
  end
end
