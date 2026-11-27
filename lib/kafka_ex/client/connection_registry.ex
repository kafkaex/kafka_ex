defmodule KafkaEx.Client.ConnectionRegistry do
  @moduledoc """
  One per client, the first child of its supervisor. It starts the client's data, coordinator and
  fetch connections and lists them as `{{host, port, role}, pid}` in a protected ETS table that only
  it writes. The metadata store keeps its own `:metadata` connections.
  """
  use GenServer

  alias KafkaEx.Client
  alias KafkaEx.Client.ConnectionStateMachine

  @type key :: {binary(), non_neg_integer(), ConnectionStateMachine.role()}

  @spec start_link(map()) :: GenServer.on_start()
  def start_link(args), do: GenServer.start_link(__MODULE__, args)

  @spec connect(pid(), key(), timeout()) :: {:ok, pid()} | :restarting
  def connect(registry, key, timeout), do: GenServer.call(registry, {:connect, key}, timeout)

  @spec unregister(pid(), key()) :: :ok
  def unregister(registry, key), do: GenServer.call(registry, {:unregister, key})

  @spec table(pid(), timeout()) :: :ets.tid()
  def table(registry, timeout), do: GenServer.call(registry, :table, timeout)

  @impl true
  def init(args) do
    table = :ets.new(:conns, [:set, :protected, read_concurrency: true])
    {:ok, Map.put(args, :table, table)}
  end

  @impl true
  def handle_call({:connect, key}, _from, state), do: {:reply, connection(key, state), state}

  # Deletes only the caller's own row, so a late unregister never removes the connection that replaced it.
  def handle_call({:unregister, key}, {pid, _tag}, state) do
    :ets.delete_object(state.table, {key, pid})
    {:reply, :ok, state}
  end

  def handle_call(:table, _from, state), do: {:reply, state.table, state}

  @impl true
  def handle_info({:DOWN, _ref, :process, pid, _reason}, state) do
    :ets.match_delete(state.table, {:_, pid})
    {:noreply, state}
  end

  # A row outlives its connection until the :DOWN arrives, so only a live pid is a hit.
  defp connection(key, state) do
    case :ets.lookup(state.table, key) do
      [{^key, pid}] -> if(Process.alive?(pid), do: {:ok, pid}, else: start(key, state))
      [] -> start(key, state)
    end
  end

  defp start({host, port, role} = key, state) do
    args = Map.merge(state.connection, %{client: state.client, registry: self(), host: host, port: port, role: role})

    case Client.start_connection(state.supervisor, args) do
      {:ok, pid} ->
        Process.monitor(pid)
        :ets.insert(state.table, {key, pid})
        {:ok, pid}

      :restarting ->
        :restarting
    end
  end
end
