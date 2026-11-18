defmodule KafkaEx.TestSupport.LeakProbe do
  @moduledoc false

  @sys_timeout 500
  @full [limit: :infinity, printable_limit: :infinity]

  def surfaces(pids) do
    for pid <- pids, {surface, text} <- read(pid), do: {pid, surface, text}
  end

  def forms(secret), do: [secret, inspect(secret, @full), to_string(:io_lib.format(~c"~p", [secret]))]

  def exposures(texts, secrets) do
    for {label, text} <- texts,
        is_binary(text),
        {kind, secret} <- secrets,
        secret != "",
        Enum.any?(forms(secret), &String.contains?(text, &1)),
        into: MapSet.new(),
        do: Tuple.insert_at(label, tuple_size(label), kind)
  end

  def render(term), do: [inspect(term, @full), to_string(:io_lib.format(~c"~p", [term]))]

  defp read(pid) do
    [state_inspect, state_p] = sys(pid, &:sys.get_state/2)
    [status_inspect, status_p] = sys(pid, &:sys.get_status/2)
    [state_inspect: state_inspect, state_p: state_p, status_inspect: status_inspect, status_p: status_p]
  end

  defp sys(pid, fun) do
    render(fun.(pid, @sys_timeout))
  catch
    :exit, _reason -> [:unavailable, :unavailable]
  end
end
