defmodule KafkaEx.Client.Supervisor do
  @moduledoc """
  One per client, started by and linked to it, so stopping or crashing the client takes all its
  processes with it. Its child `:connections` is a `DynamicSupervisor` of connection processes, one
  per broker and role. It also supervises the client's metadata store, started after the connections,
  so `rest_for_one` restarts the store when the connection supervisor restarts.

  It restarts `rest_for_one`, at most 3 times in 5 seconds; past that it exits and takes the client
  with it, so the application's supervisor restarts the client. `stop/3` unlinks first and kills
  the supervisor if it does not stop in time.
  """
  use Supervisor

  alias KafkaEx.Client.MetadataStore

  @spec start_link(%{optional(:store) => map()} | []) :: Supervisor.on_start()
  def start_link(arg \\ []), do: Supervisor.start_link(__MODULE__, arg)

  @impl true
  def init(arg) do
    children = [
      %{id: :connections, start: {DynamicSupervisor, :start_link, [[strategy: :one_for_one]]}, type: :supervisor}
      | store(arg)
    ]

    Supervisor.init(children, strategy: :rest_for_one, max_restarts: 3, max_seconds: 5)
  end

  defp store(%{store: store}) do
    start = {MetadataStore, :start_link, [Map.put(store, :supervisor, self())]}
    [%{id: :metadata_store, start: start, restart: :transient, shutdown: 5_000}]
  end

  defp store(_arg), do: []

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
  def connection_supervisor(supervisor), do: child(supervisor, :connections)

  @spec metadata_store(pid()) :: pid() | nil
  def metadata_store(supervisor), do: child(supervisor, :metadata_store)

  defp child(supervisor, id) do
    Enum.find_value(Supervisor.which_children(supervisor), fn
      {^id, pid, _, _} when is_pid(pid) -> pid
      _ -> nil
    end)
  end
end
