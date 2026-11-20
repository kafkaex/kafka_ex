defmodule KafkaEx.Client.RequestBudget do
  @moduledoc """
  Outer `GenServer.call` budget for a client request, derived from its per-attempt
  `Socket.recv` deadline. Pure (config is passed in) so it stays testable in
  isolation — the budget the capturing-mock API tests can't observe.
  """

  @call_buffer 5_000

  @doc """
  `per_attempt * attempts + buffer`, where `attempts` is the client retry count
  (or `1`, the default, for a send-once request).

  The product is a safe upper bound, not a wall-clock guarantee: a socket timeout
  is send-once, so the client rarely consumes more than one `per_attempt`.
  """
  @spec call_budget(pos_integer(), pos_integer()) :: pos_integer()
  def call_budget(per_attempt, attempts \\ 1)
      when is_integer(per_attempt) and per_attempt > 0 and is_integer(attempts) and attempts > 0 do
    per_attempt * attempts + @call_buffer
  end

  @reply_margin 1_000

  @doc "The share of a `GenServer.call` timeout the client may spend, so its reply beats the caller's timeout."
  @spec deadline_budget(pos_integer()) :: non_neg_integer()
  def deadline_budget(call_timeout) when is_integer(call_timeout) and call_timeout > 0,
    do: call_timeout - max(1, min(@reply_margin, div(call_timeout, 2)))

  @spec put_call_budget(Keyword.t(), pos_integer()) :: Keyword.t()
  def put_call_budget(opts, call_timeout),
    do: Keyword.put(opts, :call_budget, {deadline_budget(call_timeout), System.monotonic_time(:millisecond)})
end
