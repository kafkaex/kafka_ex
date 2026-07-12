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
      for produce (duplicate writes). Only produce overrides the default with
      `&KafkaEx.Support.Retry.produce_retryable?/1`; everything else inherits
      always-retry. `KafkaEx.Support.Retry` centralizes error-code
      classification for the separate `with_retry` loop (stream/consumer);
      converging the two is deferred follow-up.

    * `network_timeout` is the **per-attempt** `Socket.recv` deadline. It is set
      explicitly by the requests the broker legitimately holds — fetch (derived
      from `:max_wait_time`), JoinGroup (`rebalance_timeout + 5000`) and SyncGroup
      (session window) — each of which widens its matching `GenServer.call` budget
      in `KafkaEx.API` so the two cannot mismatch (issue #357). Leave `nil` for
      every other request so it falls back to the generic `:request_timeout`
      (`KafkaEx.Config.request_timeout/0`); those callers derive their outer
      `GenServer.call` budget from the same value. It is per-attempt, NOT the
      total budget.

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
    network_timeout: nil
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
          network_timeout: non_neg_integer() | nil
        }
end
