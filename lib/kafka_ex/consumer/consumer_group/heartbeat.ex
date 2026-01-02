defmodule KafkaEx.Consumer.ConsumerGroup.Heartbeat do
  @moduledoc false

  # GenServer to send heartbeats to the broker
  #
  # A `HeartbeatRequest` is sent periodically by each active group member (after
  # completing the join/sync phase) to inform the broker that the member is
  # still alive and participating in the group. If a group member fails to send
  # a heartbeat before the group's session timeout expires, the coordinator
  # removes that member from the group and initiates a rebalance.
  #
  # `HeartbeatResponse` allows the coordinating broker to communicate the
  # group's status to each member:
  #
  #   * `:no_error` indicates that the group is up to date and no action is
  #   needed.
  #   * `:rebalance_in_progress` a rebalance has been initiated, so each member
  #   should re-join.
  #   * `:unknown_member_id` means that this heartbeat was from a previous dead
  #   generation.
  #
  # For either of the error conditions, the heartbeat process exits, which is
  # trapped by the KafkaEx.Consumer.ConsumerGroup.Manager and handled by re-joining the
  # consumer group. (see KafkaEx.Consumer.ConsumerGroup.Manager.join/1)

  use GenServer
  require Logger
  alias KafkaEx.API, as: KafkaExAPI

  defmodule State do
    @moduledoc false
    defstruct [:client, :group_name, :member_id, :generation_id, :heartbeat_interval]
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

      {:error, :unknown_member_id} ->
        {:stop, {:shutdown, :rebalance}, state}

      {:error, reason} ->
        Logger.warning("Heartbeat failed, got error reason #{inspect(reason)}")
        {:stop, {:shutdown, {:error, reason}}, state}
    end
  end
end
