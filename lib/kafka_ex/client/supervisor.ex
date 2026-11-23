defmodule KafkaEx.Client.Supervisor do
  @moduledoc """
  One per client, started by and linked to it, so stopping or crashing the client takes all its
  processes with it. Its child `:connections` is a `DynamicSupervisor` of connection processes, one
  per broker and role.

  It restarts `rest_for_one`, at most 3 times in 5 seconds; past that it exits and takes the client
  with it, so the application's supervisor restarts the client. `stop/3` unlinks first and kills
  the supervisor if it does not stop in time.
  """
  use Supervisor

  def start_link(arg \\ []), do: Supervisor.start_link(__MODULE__, arg)

  @impl true
  def init(_arg) do
    children = [
      %{id: :connections, start: {DynamicSupervisor, :start_link, [[strategy: :one_for_one]]}, type: :supervisor}
    ]

    Supervisor.init(children, strategy: :rest_for_one, max_restarts: 3, max_seconds: 5)
  end

  # Unlinked first: a caller that does not trap exits would die of the supervisor's exit, or of its kill.
  @spec stop(pid(), term(), timeout()) :: :ok
  def stop(supervisor, reason, timeout) do
    Process.unlink(supervisor)
    Supervisor.stop(supervisor, reason, timeout)
  catch
    :exit, {:timeout, _call} ->
      Process.exit(supervisor, :kill)
      :ok

    :exit, _reason ->
      :ok
  end

  @spec connection_supervisor(pid()) :: pid() | nil
  def connection_supervisor(supervisor) do
    Enum.find_value(Supervisor.which_children(supervisor), fn
      {:connections, pid, _, _} when is_pid(pid) -> pid
      _ -> nil
    end)
  end
end
