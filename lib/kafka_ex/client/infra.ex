defmodule KafkaEx.Client.Infra do
  @moduledoc false
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
    start = {MetadataStore, :start_link, [Map.put(store, :infra, self())]}
    [%{id: :metadata_store, start: start, restart: :transient, shutdown: 5_000}]
  end

  defp store(_arg), do: []

  # Unlinked first: a caller that does not trap exits would die of Infra's exit, or of its kill.
  @spec stop(pid(), term(), timeout()) :: :ok
  def stop(infra, reason, timeout) do
    Process.unlink(infra)
    Supervisor.stop(infra, reason, timeout)
  catch
    :exit, {:timeout, _call} ->
      Process.exit(infra, :kill)
      :ok

    :exit, _reason ->
      :ok
  end

  @spec connection_supervisor(pid()) :: pid() | nil
  def connection_supervisor(infra), do: child(infra, :connections)

  @spec metadata_store(pid()) :: pid() | nil
  def metadata_store(infra), do: child(infra, :metadata_store)

  defp child(infra, id) do
    Enum.find_value(Supervisor.which_children(infra), fn
      {^id, pid, _, _} when is_pid(pid) -> pid
      _ -> nil
    end)
  end
end
