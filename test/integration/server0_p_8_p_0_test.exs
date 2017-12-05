defmodule KafkaEx.Server0P9P0.Test do
  use ExUnit.Case
  import TestHelper

  @moduletag :server_0_p_8_p_0

  alias KafkaEx.Server0P8P0, as: Server

  test "can fetch" do
    {:ok, args} = KafkaEx.build_worker_options([])
    {:ok, worker} = Server.start_link(args, :no_name)
    
    Process.unlink(worker)
    GenServer.stop(worker)
    refute Process.alive?(worker)
  end
end
