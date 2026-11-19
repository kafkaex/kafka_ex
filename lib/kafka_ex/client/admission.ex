defmodule KafkaEx.Client.Admission do
  @moduledoc false

  @type budget :: {non_neg_integer(), integer()}

  @spec admit(term(), boolean(), integer(), non_neg_integer()) :: {term(), integer()}
  def admit({:metadata, topics, opts, api_version}, local?, now, default) when is_list(opts) do
    {budget, opts} = pop_budget(opts)
    {{:metadata, topics, opts, api_version}, deadline(budget, local?, now, default)}
  end

  # The old shape took any term as the creation flag and passed it on as given.
  def admit({:topic_metadata, topics, allow}, local?, now, default) when not is_list(allow) do
    admit({:topic_metadata, topics, [allow_topic_creation: allow]}, local?, now, default)
  end

  def admit({:network_request, request, selector}, local?, now, default) do
    admit({:network_request, request, selector, []}, local?, now, default)
  end

  def admit(message, local?, now, default) when is_tuple(message) and tuple_size(message) > 1 do
    last = tuple_size(message) - 1
    opts = elem(message, last)

    if is_list(opts) do
      {budget, opts} = pop_budget(opts)
      {put_elem(message, last, opts), deadline(budget, local?, now, default)}
    else
      {message, now + default}
    end
  end

  def admit(message, _local?, now, default), do: {message, now + default}

  defp pop_budget(opts) do
    case Enum.split_with(opts, &match?({:call_budget, _}, &1)) do
      {[{:call_budget, budget} | _], rest} -> {budget, rest}
      {[], rest} -> {nil, rest}
    end
  end

  # A send time is comparable only on the caller's own node, where every process shares one monotonic clock.
  @spec deadline(budget() | term(), boolean(), integer(), non_neg_integer()) :: integer()
  def deadline({ms, sent_at}, true, _now, _default) when is_integer(ms) and is_integer(sent_at), do: sent_at + ms
  def deadline({ms, sent_at}, false, now, _default) when is_integer(ms) and is_integer(sent_at), do: now + ms
  def deadline(_unusable, _local?, now, default), do: now + default
end
