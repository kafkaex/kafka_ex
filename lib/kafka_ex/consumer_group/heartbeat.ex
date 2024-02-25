defmodule KafkaEx.ConsumerGroup.Heartbeat do
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
  # trapped by the KafkaEx.ConsumerGroup.Manager and handled by re-joining the
  # consumer group. (see KafkaEx.ConsumerGroup.Manager.join/1)

  use GenServer
  require Logger
  alias KafkaEx.Protocol.Heartbeat.Request, as: HeartbeatRequest
  alias KafkaEx.Protocol.Heartbeat.Response, as: HeartbeatResponse

  defmodule State do
    @moduledoc false
    defstruct [:worker_name, :heartbeat_request, :heartbeat_interval]
  end

  def start_link(options) do
    GenServer.start_link(__MODULE__, options)
  end

  def init(%{
        group_name: group_name,
        member_id: member_id,
        generation_id: generation_id,
        worker_name: worker_name,
        heartbeat_interval: heartbeat_interval
      }) do
    heartbeat_request = %HeartbeatRequest{
      group_name: group_name,
      member_id: member_id,
      generation_id: generation_id
    }

    state = %State{
      worker_name: worker_name,
      heartbeat_request: heartbeat_request,
      heartbeat_interval: heartbeat_interval
    }

    {:ok, state, state.heartbeat_interval}
  end

  def handle_info(
        :timeout,
        %State{
          worker_name: worker_name,
          heartbeat_request: heartbeat_request,
          heartbeat_interval: heartbeat_interval
        } = state
      ) do
    case KafkaEx.heartbeat(heartbeat_request, worker_name: worker_name) do
      %HeartbeatResponse{error_code: :no_error} ->
        {:noreply, state, heartbeat_interval}

      %HeartbeatResponse{error_code: :rebalance_in_progress} ->
        {:stop, {:shutdown, :rebalance}, state}

      %HeartbeatResponse{error_code: :unknown_member_id} ->
        {:stop, {:shutdown, :rebalance}, state}

      %HeartbeatResponse{error_code: error_code} ->
        Logger.warning("Heartbeat failed, got error code #{error_code}")
        {:stop, {:shutdown, {:error, error_code}}, state}

      {:error, reason} ->
        Logger.warning("Heartbeat failed, got error reason #{inspect(reason)}")
        {:stop, {:shutdown, {:error, reason}}, state}
    end
  end
end
