defmodule KafkaEx.Consumer.ConsumerGroup.Heartbeat do
  @moduledoc """
   GenServer to send heartbeats to the broker

   A `HeartbeatRequest` is sent periodically by each active group member (after
   completing the join/sync phase) to inform the broker that the member is
   still alive and participating in the group. If a group member fails to send
   a heartbeat before the group's session timeout expires, the coordinator
   removes that member from the group and initiates a rebalance.

   `HeartbeatResponse` allows the coordinating broker to communicate the
   group's status to each member:

     * `:no_error` indicates that the group is up to date and no action is
     needed.
     * `:rebalance_in_progress` a rebalance has been initiated, so each member
     should re-join.
     * `:unknown_member_id` means the coordinator has forgotten this member; it
     must re-join with a fresh (reset) member_id.
     * `:illegal_generation` means this member's generation is stale; it must
     re-join (keeping its member_id) to pick up the new generation.
     * `:fenced_instance_id` (another member claimed this member's static
     `group.instance.id`) and `:group_authorization_failed` (no access to the
     group) are terminal; the member stops rather than re-joining.

   For the recoverable conditions the heartbeat process exits with a
   `{:shutdown, _}` reason that the KafkaEx.Consumer.ConsumerGroup.Manager
   traps and turns into a re-join (resetting identity per the reason) or a
   clean terminal stop. (see KafkaEx.Consumer.ConsumerGroup.Manager)
  """

  use GenServer
  require Logger
  alias KafkaEx.API, as: KafkaExAPI

  defmodule State do
    @moduledoc false
    defstruct [:client, :group_name, :member_id, :generation_id, :heartbeat_interval]

    @type t :: %__MODULE__{
            client: pid(),
            group_name: binary(),
            member_id: binary(),
            generation_id: integer(),
            heartbeat_interval: pos_integer()
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
        heartbeat_interval: heartbeat_interval
      }) do
    state = %State{
      client: client,
      group_name: group_name,
      member_id: member_id,
      generation_id: generation_id,
      heartbeat_interval: heartbeat_interval
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
          heartbeat_interval: heartbeat_interval
        } = state
      ) do
    case KafkaExAPI.heartbeat(client, group_name, member_id, generation_id) do
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
      {:stop, {:shutdown, {:terminal, {:crashed, :error}}}, state}
  catch
    :throw, value ->
      Logger.error("Heartbeat threw a value: #{inspect(value)}")
      {:stop, {:shutdown, {:terminal, {:crashed, :throw}}}, state}

    :exit, reason ->
      Logger.warning("Heartbeat client call exited (#{inspect(reason)}); stopping with a structured error")
      {:stop, {:shutdown, {:error, normalize_exit_reason(reason)}}, state}
  end

  defp normalize_exit_reason({reason, _call_context}) when is_atom(reason), do: reason
  defp normalize_exit_reason(reason), do: reason
end
