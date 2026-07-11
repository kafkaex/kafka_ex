defmodule KafkaEx.Consumer.ConsumerGroup.Heartbeat do
  @moduledoc """
  Sends periodic `HeartbeatRequest`s for a joined consumer-group member.

  The Manager starts this process **linked** and trapping exits. Every failure
  this process can catch is turned into a structured `{:shutdown, reason}` exit
  that the Manager maps to a rejoin or a terminal stop. The one failure it
  cannot catch — `:kill` (untrappable; no callback runs) — reaches the Manager
  as a plain `:killed` EXIT.

  ## Lifecycle

      Manager ──start_link (linked)──▶ Heartbeat
                                          │  loop: every heartbeat_interval
                                          ▼
                            send HeartbeatRequest to the broker
                                          │
                       :no_error ◀────────┴────────▶ error / crash
                          │                              │
                    keep ticking                exit {:shutdown, reason}

      exit reason                                  ─▶ Manager action
      ────────────────────────────────────────────────────────────────
      :rebalance                                   ─▶ rejoin
      {:rejoin, :illegal_generation}               ─▶ rejoin, keep member_id
      {:rejoin, :unknown_member_id}                ─▶ rejoin, reset member_id
      {:error, recoverable}  (e.g. :timeout)       ─▶ rejoin
      {:error, non-recoverable} | {:terminal, _}   ─▶ terminal stop
      :killed  (uncatchable)                       ─▶ rejoin

  See `KafkaEx.Consumer.ConsumerGroup.Manager` for the EXIT-clause handling.
  """

  use GenServer
  require Logger
  alias KafkaEx.API, as: KafkaExAPI

  defmodule State do
    @moduledoc false
    defstruct [:client, :group_name, :member_id, :generation_id, :heartbeat_interval, :group_instance_id]

    @type t :: %__MODULE__{
            client: pid(),
            group_name: binary(),
            member_id: binary(),
            generation_id: integer(),
            heartbeat_interval: pos_integer(),
            group_instance_id: binary() | nil
          }
  end

  def start_link(options) do
    GenServer.start_link(__MODULE__, options)
  end

  def init(%{
        group_name: group_name,
        member_id: member_id,
        generation_id: generation_id,
        client: client,
        heartbeat_interval: heartbeat_interval,
        group_instance_id: group_instance_id
      }) do
    state = %State{
      client: client,
      group_name: group_name,
      member_id: member_id,
      generation_id: generation_id,
      heartbeat_interval: heartbeat_interval,
      group_instance_id: group_instance_id
    }

    {:ok, state, state.heartbeat_interval}
  end

  def handle_info(
        :timeout,
        %State{
          client: client,
          group_name: group_name,
          member_id: member_id,
          generation_id: generation_id,
          heartbeat_interval: heartbeat_interval,
          group_instance_id: group_instance_id
        } = state
      ) do
    # Heartbeat is answered promptly by the coordinator (never held), so bound its
    # per-attempt socket-recv to heartbeat_interval rather than the generic
    # :request_timeout — a stalled heartbeat is then detected well inside
    # session_timeout, leaving the manager time to rejoin before eviction.
    heartbeat_opts = [group_instance_id: group_instance_id, network_timeout: heartbeat_interval]

    case KafkaExAPI.heartbeat(client, group_name, member_id, generation_id, heartbeat_opts) do
      {:ok, %KafkaEx.Messages.Heartbeat{}} ->
        {:noreply, state, heartbeat_interval}

      {:error, :rebalance_in_progress} ->
        {:stop, {:shutdown, :rebalance}, state}

      {:error, :illegal_generation} ->
        {:stop, {:shutdown, {:rejoin, :illegal_generation}}, state}

      {:error, :unknown_member_id} ->
        {:stop, {:shutdown, {:rejoin, :unknown_member_id}}, state}

      {:error, reason} when reason in [:fenced_instance_id, :group_authorization_failed] ->
        Logger.error("Heartbeat hit terminal error #{inspect(reason)}; stopping without rejoin")
        {:stop, {:shutdown, {:terminal, reason}}, state}

      {:error, reason} ->
        Logger.warning("Heartbeat failed, got error reason #{inspect(reason)}")
        {:stop, {:shutdown, {:error, reason}}, state}
    end
  rescue
    exception ->
      Logger.error("Heartbeat crashed: " <> Exception.format(:error, exception, __STACKTRACE__))
      {:stop, {:shutdown, {:terminal, {:crashed, exception.__struct__}}}, state}
  catch
    :exit, reason ->
      Logger.warning("Heartbeat client call exited (#{inspect(reason)})")
      {:stop, {:shutdown, {:error, normalize_exit_reason(reason)}}, state}
  end

  defp normalize_exit_reason({reason, {GenServer, :call, _}}) when is_atom(reason), do: reason
  defp normalize_exit_reason(reason), do: reason
end
