defmodule KafkaEx.Supervisor.Test do
  use ExUnit.Case, async: false

  @restart_elem 10

  test "uses configured max_restarts" do
    # stop the application, configure supervision max_restarts and restart the application
    configured_max_restarts = 15

    :ok = Application.stop(:kafka_ex)
    Application.put_env(:kafka_ex, :max_restarts, configured_max_restarts)
    {:ok, _} = Application.ensure_all_started(:kafka_ex)
    [max_restarts, _max_seconds] = :sys.get_state(KafkaEx.Supervisor) |> elem(@restart_elem)

    assert configured_max_restarts == max_restarts

    # Delete the config and restart the application
    :ok = Application.stop(:kafka_ex)
    Application.delete_env(:kafka_ex, :max_restarts)
    {:ok, _} = Application.ensure_all_started(:kafka_ex)
    [max_restarts, _max_seconds] = :sys.get_state(KafkaEx.Supervisor) |> elem(@restart_elem)

    refute configured_max_restarts == max_restarts
  end

  test "uses configured max_seconds" do
    # stop the application, configure supervision max_seconds and restart the application
    configured_max_seconds = 120

    :ok = Application.stop(:kafka_ex)
    Application.put_env(:kafka_ex, :max_seconds, configured_max_seconds)
    {:ok, _} = Application.ensure_all_started(:kafka_ex)
    [_max_restarts, max_seconds] = :sys.get_state(KafkaEx.Supervisor) |> elem(@restart_elem)

    assert configured_max_seconds == max_seconds

    # Delete the config and restart the application
    :ok = Application.stop(:kafka_ex)
    Application.delete_env(:kafka_ex, :max_seconds)
    {:ok, _} = Application.ensure_all_started(:kafka_ex)
    [_max_restarts, max_seconds] = :sys.get_state(KafkaEx.Supervisor) |> elem(@restart_elem)

    refute configured_max_seconds == max_seconds
  end
end
