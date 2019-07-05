defmodule KafkaEx.New.Broker do
  @moduledoc false

  defstruct node_id: nil, host: nil, port: nil, rack: nil, pid: nil

  def put_pid(%__MODULE__{} = broker, pid), do: %{broker | pid: pid}
end
