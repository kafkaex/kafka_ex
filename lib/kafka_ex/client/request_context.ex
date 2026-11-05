defmodule KafkaEx.Client.RequestContext do
  @moduledoc """
  Invariant inputs for one logical client request, bundled so the retry
  functions in `KafkaEx.Client` (`handle_request_with_retry`,
  `handle_request_error`) stay small.

  The retry loop varies only `state`, `retry_count`, and `last_error`;
  everything in this struct is fixed for the life of the request. It merges the
  three concerns the loop needs together:

    * request identity/payload — `request`, `node_selector`
    * response projection — `parser_fn` (deserialized response -> domain result)
    * retry policy — `retryable?`

  ## Design notes

    * `retryable?` is keyed **per request**, not per error. The reference Kafka
      clients (Java `RetriableException`, KafkaJS `error.retriable`, librdkafka)
      classify retriability on the error, but that cannot work here: the same
      error atom (e.g. `:request_timed_out`) is retryable for fetch yet unsafe
      for produce (duplicate writes). The default is
      `&KafkaEx.Support.Retry.data_plane_retryable?/1`; produce narrows it to
      `produce_retryable?/1`, and the consumer-group operations use their own
      classifiers. All of them live in `KafkaEx.Support.Retry` — the single home
      for error classification, shared with the `with_retry` loop (stream/consumer).

    * `network_timeout` is the **per-attempt** `Socket.recv` deadline. It is set
      explicitly by the requests that need a non-generic deadline — fetch (from
      `:max_wait_time`), JoinGroup (rebalance_timeout + grace), SyncGroup (session
      window), Heartbeat (heartbeat_interval) and CreateTopics/DeleteTopics (broker
      op timeout + grace) — each widening its matching `GenServer.call` budget in
      `KafkaEx.API` so the two cannot mismatch (issue #357). Leave `nil` for every
      other request so it falls back to the generic `:request_timeout`
      (`KafkaEx.Config.request_timeout/0`). It is per-attempt, NOT the total budget.

    * `rediscover_coordinator?` (default `false`) - for a coordinator request,
      a connection-class failure (`KafkaEx.Support.Retry.coordinator_connection_error?/1`)
      also drops the cached group coordinator, so the next attempt (or the
      next call, after a send-once `:timeout`) runs FindCoordinator instead
      of retrying a broker that may be gone. Java's consumer marks the
      coordinator unknown on any disconnect the same way. Only the KIP-848
      ConsumerGroupHeartbeat sets it; the classic requests keep their
      behaviour.

    * `send_once?` (default `&KafkaEx.Support.Retry.transport_timeout?/1`) - for
      a coordinator request, the errors after which the request is NOT resent
      by the client, because the broker may already have processed it: it is
      returned to the caller, which re-issues it. The default is the recv
      deadline (`:timeout`). A KIP-848 member heartbeat (epoch > 0) also
      returns a lost connection or reply (`Retry.consumer_group_heartbeat_send_once?/1`):
      an incremental heartbeat resent unchanged carries an epoch the broker
      may already have bumped and is fenced, so the manager resends it as a
      full request with the owned partitions (Java resets the heartbeat
      state on any disconnect and never resends a request).

    * Backoff/jitter between attempts is intentionally absent — the client loop
      retries immediately (refreshing metadata on leadership errors). If added
      later, a retry-policy field on this struct is the seam.
  """

  alias KafkaEx.Client.NodeSelector
  alias KafkaEx.Support.Retry

  @enforce_keys [:request, :parser_fn, :node_selector]
  defstruct [
    :request,
    :parser_fn,
    :node_selector,
    retryable?: &Retry.data_plane_retryable?/1,
    network_timeout: nil,
    rediscover_coordinator?: false,
    send_once?: &Retry.transport_timeout?/1
  ]

  @typedoc "Projects a deserialized response into a domain result."
  @type parser_fn :: (term() -> {:ok, term()} | {:error, term()})

  @typedoc "Decides whether an error should trigger a retry."
  @type retryable_fn :: (term() -> boolean())

  @type t :: %__MODULE__{
          request: struct(),
          parser_fn: parser_fn(),
          node_selector: NodeSelector.t(),
          retryable?: retryable_fn(),
          network_timeout: non_neg_integer() | nil,
          rediscover_coordinator?: boolean(),
          send_once?: (atom() -> boolean())
        }
end
