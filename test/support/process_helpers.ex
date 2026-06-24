defmodule KafkaEx.TestSupport.ProcessHelpers do
  @moduledoc """
  Safe process teardown for tests.

  Tests `start_link` processes that are linked to the test process; when the
  test ends those processes die with it, and `on_exit` runs afterwards. A plain
  `GenServer.stop/1` (or `Supervisor.stop/1`) then raises `(EXIT) :noproc` on the
  already-dead process, and `if Process.alive?(pid), do: GenServer.stop(pid)` is a
  TOCTOU that does not close the race. `stop_safely/1` swallows the failure so
  teardown never fails on a dead process. Works for any OTP process (GenServer or
  Supervisor) — both are `:gen` processes.

  It also accepts a name (atom, `{:global, _}`, `{:via, _, _}`). A name whose
  process or backing registry is already gone resolves to an exit (`:noproc`) or
  a raise (e.g. `ArgumentError: unknown registry` from a torn-down `Registry`);
  both are caught, so any teardown reason — and any abnormal exit reason passed
  in — yields `:ok`.
  """

  @doc """
  Stop an OTP process, returning `:ok` even if it is already dead, dying, or
  named by a process/registry that no longer exists. Use in `on_exit/1` and
  best-effort cleanup instead of `GenServer.stop`/`Supervisor.stop`.
  """
  @spec stop_safely(GenServer.server(), term(), timeout()) :: :ok
  def stop_safely(server, reason \\ :normal, timeout \\ :infinity) do
    _ = GenServer.stop(server, reason, timeout)
    :ok
  rescue
    _ -> :ok
  catch
    :exit, _ -> :ok
  end
end
