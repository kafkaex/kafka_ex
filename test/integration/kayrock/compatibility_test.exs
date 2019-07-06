defmodule KafkaEx.KayrockCompatibilityTest do
  @moduledoc """
  These are tests using the original KafkaEx API with the kayrock server
  """

  use ExUnit.Case

  @moduletag :server_kayrock

  alias KafkaEx.ServerKayrock
  alias KafkaEx.Protocol.Offset.Response, as: OffsetResponse

  setup do
    {:ok, args} = KafkaEx.build_worker_options([])

    {:ok, pid} = ServerKayrock.start_link(args)

    {:ok, %{client: pid}}
  end

  test "list offsets", %{client: client} do
    topic = "test0p8p0"

    {:ok, resp} = KafkaEx.offset(topic, 0, :earliest, client)

    [%OffsetResponse{topic: ^topic, partition_offsets: [partition_offsets]}] =
      resp

    %{error_code: :no_error, offset: [offset], partition: 0} = partition_offsets
    assert offset >= 0
  end

  # test "produce/4 without an acq required returns :ok", %{client: client} do
  #  assert KafkaEx.produce("food", 0, "hey", client) == :ok
  # end
end
