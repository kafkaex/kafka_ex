defmodule KafkaEx.Client.Infra do
  @moduledoc false
  use Supervisor

  def start_link(arg \\ []), do: Supervisor.start_link(__MODULE__, arg)

  @impl true
  def init(_arg) do
    children = [
      %{id: :connections, start: {DynamicSupervisor, :start_link, [[strategy: :one_for_one]]}, type: :supervisor}
    ]

    Supervisor.init(children, strategy: :rest_for_one, max_restarts: 3, max_seconds: 5)
  end

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
  def connection_supervisor(infra) do
    Enum.find_value(Supervisor.which_children(infra), fn
      {:connections, pid, _, _} when is_pid(pid) -> pid
      _ -> nil
    end)
  end
end
