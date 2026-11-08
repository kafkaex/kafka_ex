defmodule KafkaEx.Protocol.Kayrock.SyncGroup.ResponseDeserializerTest do
  use ExUnit.Case, async: true

  alias KafkaEx.Protocol.Kayrock.SyncGroup.ResponseDeserializer
  alias KafkaEx.Protocol.KayrockProtocol
  alias Kayrock.MemberAssignment
  alias Kayrock.MemberAssignment.PartitionAssignment
  alias Kayrock.SyncGroup

  # ConsumerProtocolAssignment v0 for topic "t" partitions [0, 3], user data
  # as given (`nil` -> null, length -1).
  defp assignment(user_data) do
    tail =
      case user_data do
        nil -> <<-1::32-signed>>
        bin -> <<byte_size(bin)::32-signed, bin::binary>>
      end

    <<0::16, 1::32, 1::16, "t", 2::32, 0::32, 3::32, tail::binary>>
  end

  defp v0(assignment), do: <<7::32, 0::16, byte_size(assignment)::32, assignment::binary>>
  defp v1(assignment), do: <<7::32, 25::32, 0::16, byte_size(assignment)::32, assignment::binary>>

  # Header tags (none), throttle, error, compact bytes (length + 1), tags.
  defp v4(assignment),
    do: <<7::32, 0, 25::32, 0::16, byte_size(assignment) + 1, assignment::binary, 0>>

  @requests [
    {%SyncGroup.V0.Request{}, &__MODULE__.v0_frame/1},
    {%SyncGroup.V1.Request{}, &__MODULE__.v1_frame/1},
    {%SyncGroup.V2.Request{}, &__MODULE__.v1_frame/1},
    {%SyncGroup.V3.Request{}, &__MODULE__.v1_frame/1},
    {%SyncGroup.V4.Request{}, &__MODULE__.v4_frame/1}
  ]

  def v0_frame(a), do: v0(a)
  def v1_frame(a), do: v1(a)
  def v4_frame(a), do: v4(a)

  describe "same result as Kayrock where Kayrock can decode" do
    for {request, frame} <- @requests, user_data <- ["", "some-user-data"] do
      @request request
      @frame frame
      @user_data user_data
      test "#{inspect(request.__struct__)} with user_data #{inspect(user_data)}" do
        wire = @frame.(assignment(@user_data))

        assert ResponseDeserializer.for_request(@request).(wire) ==
                 Kayrock.Request.response_deserializer(@request).(wire)
      end
    end

    for {request, frame} <- @requests do
      @request request
      @frame frame
      test "#{inspect(request.__struct__)} with an empty assignment (error response)" do
        wire = @frame.(<<>>)

        assert ResponseDeserializer.for_request(@request).(wire) ==
                 Kayrock.Request.response_deserializer(@request).(wire)
      end
    end
  end

  describe "null user_data (a Kafka 4.x coordinator of a migrating group)" do
    for {request, frame} <- @requests do
      @request request
      @frame frame
      test "#{inspect(request.__struct__)} decodes it as \"\"; Kayrock raises" do
        wire = @frame.(assignment(nil))

        assert_raise FunctionClauseError, fn -> Kayrock.Request.response_deserializer(@request).(wire) end

        assert {%{error_code: 0, assignment: %MemberAssignment{} = assignment}, <<>>} =
                 KayrockProtocol.response_deserializer(@request).(wire)

        assert assignment.user_data == ""
        assert [%PartitionAssignment{topic: "t", partitions: partitions}] = assignment.partition_assignments
        assert Enum.sort(partitions) == [0, 3]
      end
    end

    test "the exact SyncGroup v4 response Kafka 4.4.0 sent a classic member of a converted group" do
      group = "kafka4_migrate_YECVCGGFPKFWCMKDVPRC"

      # Captured from apache/kafka:4.4.0 (empty partition list, null user data).
      wire =
        <<0, 0, 0, 13, 0, 0, 0, 0, 0, 0, 0, 60, 0, 0, 0, 0, 0, 1, 0, 35>> <>
          "kafka4_migrate_UKICWKBMKTQACGEIBYJG" <> <<0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 1, 255, 255, 255, 255, 0>>

      request = %SyncGroup.V4.Request{group_id: group}
      assert_raise FunctionClauseError, fn -> Kayrock.Request.response_deserializer(request).(wire) end

      assert {%SyncGroup.V4.Response{correlation_id: 13, error_code: 0, assignment: assignment}, <<>>} =
               KayrockProtocol.response_deserializer(request).(wire)

      assert %MemberAssignment{
               partition_assignments: [
                 %PartitionAssignment{topic: "kafka4_migrate_UKICWKBMKTQACGEIBYJG", partitions: [1, 0]}
               ],
               user_data: ""
             } = assignment

      assert {:ok, %KafkaEx.Messages.SyncGroup{partition_assignments: [pa]}} =
               KayrockProtocol.parse_response(
                 :sync_group,
                 elem(KayrockProtocol.response_deserializer(request).(wire), 0)
               )

      assert Enum.sort(pa.partitions) == [0, 1]
    end
  end

  # Raising the SyncGroup cap without extending this decoder would bring back
  # Kayrock's, which raises on a null user_data.
  test "every SyncGroup version the client can send is decoded here" do
    for version <- 0..KayrockProtocol.max_supported_version(:sync_group) do
      request = struct(Module.concat([SyncGroup, "V#{version}", Request]))
      deserializer = KayrockProtocol.response_deserializer(request)
      assert Function.info(deserializer)[:module] == ResponseDeserializer, "SyncGroup v#{version}"
    end
  end

  test "other requests keep Kayrock's deserializer" do
    request = %Kayrock.Heartbeat.V0.Request{}
    wire = <<1::32, 0::16>>

    assert KayrockProtocol.response_deserializer(request).(wire) ==
             Kayrock.Request.response_deserializer(request).(wire)
  end
end
