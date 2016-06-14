defmodule KafkaExTest do
  use ExUnit.Case, async: false

  test "Setting disable_default_worker to true removes the KafkaEx.server worker" do
    # stop the application, disable the default worker, restart the application
    :ok = Application.stop(:kafka_ex)
    Application.put_env(:kafka_ex, :disable_default_worker, true)
    {:ok, _} = Application.ensure_all_started(:kafka_ex)

    # the supervisor should now have no children and the default worker should not be registered
    assert [] == Supervisor.which_children(KafkaEx.Supervisor)
    assert nil == Process.whereis(KafkaEx.server)

    # revert the change, restart the application
    :ok = Application.stop(:kafka_ex)
    Application.put_env(:kafka_ex, :disable_default_worker, false)
    {:ok, _} = Application.ensure_all_started(:kafka_ex)

    # we should have the default worker back again
    pid = Process.whereis(KafkaEx.server)
    assert is_pid(pid)
    assert [{:undefined, pid, :worker, [KafkaEx.server]}] ==
      Supervisor.which_children(KafkaEx.Supervisor)
  end
end
