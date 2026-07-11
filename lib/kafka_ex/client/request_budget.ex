defmodule KafkaEx.Client.RequestBudget do
  @moduledoc """
  Pure arithmetic for the outer `GenServer.call` budget of a client request,
  derived from its per-attempt `Socket.recv` deadline.

  This is a side-effect-free leaf: configuration is passed in, never read here,
  so the budget math — the exact spot the issue #357 timeout-mismatch class lives
  — is unit-testable in isolation, with no running client and no Application env.

  The *per-operation policy* (which per-attempt deadline a given request uses:
  `rebalance_timeout` for JoinGroup, the session window for SyncGroup, the broker
  op timeout for create/delete, `:max_wait_time` for fetch, `:request_timeout`
  otherwise) intentionally stays at each `KafkaEx.API` call site — only the
  shared arithmetic lives here. This mirrors the reference clients, which extract
  a deadline helper (Java `org.apache.kafka.common.utils.Timer`, librdkafka
  `rdtime.h`, brod `kpro_connection.deadline/1`) but keep the per-request policy
  next to the protocol call it guards.
  """

  # Headroom the outer GenServer.call adds over the per-attempt Socket.recv, so
  # the caller never exits before the client returns a clean {:error, :timeout}.
  @call_buffer 5_000

  @doc """
  Outer call budget for a request the client retries up to `retries` times:
  `per_attempt * retries + buffer`. Used by fetch and the generic data-plane
  wrappers, whose synchronous retry loop can consume up to `retries` attempts.
  """
  @spec retrying(pos_integer(), pos_integer()) :: pos_integer()
  def retrying(per_attempt, retries)
      when is_integer(per_attempt) and per_attempt > 0 and is_integer(retries) and retries > 0 do
    per_attempt * retries + @call_buffer
  end

  @doc """
  Outer call budget for a send-once request (no client-level retry):
  `per_attempt + buffer`. Used by the coordinator calls (JoinGroup/SyncGroup)
  and the low-level `send_request/4` helper.
  """
  @spec send_once(pos_integer()) :: pos_integer()
  def send_once(per_attempt) when is_integer(per_attempt) and per_attempt > 0 do
    per_attempt + @call_buffer
  end
end
