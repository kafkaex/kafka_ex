defmodule KafkaEx.Protocol.Kayrock.ConsumerGroupHeartbeat.RequestTest do
  use ExUnit.Case, async: true

  alias KafkaEx.Messages.TopicPartitions
  alias KafkaEx.Protocol.Kayrock.ConsumerGroupHeartbeat
  alias KafkaEx.Protocol.Kayrock.ConsumerGroupHeartbeat.RequestHelpers

  @topic_id <<1::128>>

  defp build(version, opts) do
    version
    |> Kayrock.ConsumerGroupHeartbeat.get_request_struct()
    |> ConsumerGroupHeartbeat.Request.build_request(opts)
  end

  # Serializes and re-parses the request body with Kayrock's own schema, so the
  # test checks what goes on the wire, not just the struct.
  defp wire(request) do
    request = %{request | correlation_id: 7, client_id: "test"}
    IO.iodata_to_binary(Kayrock.Request.serialize(request))
  end

  for version <- [0, 1] do
    describe "V#{version} Request (common fields)" do
      @version version

      test "a join sends the full subscription and an empty owned set" do
        request =
          build(@version,
            group_id: "g",
            member_id: "m-1",
            member_epoch: 0,
            rebalance_timeout_ms: 60_000,
            subscribed_topic_names: ["t1", "t2"],
            server_assignor: "uniform",
            topic_partitions: []
          )

        assert request.group_id == "g"
        assert request.member_id == "m-1"
        assert request.member_epoch == 0
        assert request.rebalance_timeout_ms == 60_000
        assert request.subscribed_topic_names == ["t1", "t2"]
        assert request.server_assignor == "uniform"
        assert request.topic_partitions == []
        assert is_binary(wire(request))
      end

      test "fields left out are nil (unchanged), not Kayrock's [] default" do
        request = build(@version, group_id: "g", member_id: "m-1", member_epoch: 5)

        assert request.subscribed_topic_names == nil
        assert request.topic_partitions == nil
        assert request.instance_id == nil
        assert request.rack_id == nil
        assert request.server_assignor == nil
        assert request.rebalance_timeout_ms == -1
        # nil arrays encode as compact-null (varint 0); this must serialize
        assert is_binary(wire(request))
      end

      test "an explicit nil rebalance_timeout_ms is sent as -1" do
        assert build(@version, group_id: "g", member_id: "m", member_epoch: 1, rebalance_timeout_ms: nil).rebalance_timeout_ms ==
                 -1
      end

      test "owned partitions accept TopicPartitions structs and plain maps" do
        request =
          build(@version,
            group_id: "g",
            member_id: "m",
            member_epoch: 3,
            topic_partitions: [
              %TopicPartitions{topic_id: @topic_id, topic_name: "t", partitions: [0, 2]},
              %{topic_id: <<2::128>>, partitions: [1]}
            ]
          )

        assert request.topic_partitions == [
                 %{topic_id: @topic_id, partitions: [0, 2]},
                 %{topic_id: <<2::128>>, partitions: [1]}
               ]

        assert is_binary(wire(request))
      end

      test "static membership and rack" do
        request = build(@version, group_id: "g", member_id: "m", member_epoch: -2, instance_id: "i-1", rack_id: "r1")
        assert request.instance_id == "i-1"
        assert request.rack_id == "r1"
        assert request.member_epoch == -2
      end

      test "requires member_epoch" do
        assert_raise KeyError, fn -> build(@version, group_id: "g", member_id: "m") end
      end
    end
  end

  describe "V0 member id" do
    test "a join without member_id sends \"\" (the coordinator assigns one)" do
      assert build(0, group_id: "g", member_epoch: 0).member_id == ""
    end
  end

  describe "V1 Request (regex + member id)" do
    test "carries subscribed_topic_regex" do
      request = build(1, group_id: "g", member_id: "m", member_epoch: 0, subscribed_topic_regex: "orders-.*")
      assert request.subscribed_topic_regex == "orders-.*"
      assert is_binary(wire(request))
    end

    test "subscribed_topic_regex defaults to nil" do
      assert build(1, group_id: "g", member_id: "m", member_epoch: 1).subscribed_topic_regex == nil
    end

    test "a join without member_id gets a client-generated id (KIP-1082)" do
      for member_id <- [nil, ""] do
        request = build(1, group_id: "g", member_id: member_id, member_epoch: 0)
        assert byte_size(request.member_id) == 22
      end

      assert build(1, group_id: "g", member_epoch: 0).member_id =~ ~r/^[A-Za-z0-9_-]{22}$/
    end

    test "two joins get different ids" do
      refute build(1, group_id: "g", member_epoch: 0).member_id == build(1, group_id: "g", member_epoch: 0).member_id
    end

    test "a given member id is kept on join" do
      assert build(1, group_id: "g", member_id: "mine", member_epoch: 0).member_id == "mine"
    end

    test "no id is generated outside a join" do
      assert build(1, group_id: "g", member_epoch: 4).member_id == ""
      assert build(1, group_id: "g", member_epoch: -1).member_id == ""
    end
  end

  describe "RequestHelpers.generate_member_id/0" do
    test "is a URL-safe base64 version 4 UUID" do
      id = RequestHelpers.generate_member_id()
      assert {:ok, <<_::48, 4::4, _::12, 2::2, _::62>>} = Base.url_decode64(id, padding: false)
    end
  end

  describe "Any fallback" do
    test "V1-shaped maps take the V1 path" do
      template = Map.from_struct(Kayrock.ConsumerGroupHeartbeat.V1.Request.__struct__())

      request =
        ConsumerGroupHeartbeat.Request.build_request(template,
          group_id: "g",
          member_epoch: 0,
          subscribed_topic_regex: "x"
        )

      assert request.subscribed_topic_regex == "x"
      assert byte_size(request.member_id) == 22
    end

    test "V0-shaped maps take the V0 path" do
      template = Map.from_struct(Kayrock.ConsumerGroupHeartbeat.V0.Request.__struct__())
      request = ConsumerGroupHeartbeat.Request.build_request(template, group_id: "g", member_epoch: 0)
      refute Map.has_key?(request, :subscribed_topic_regex)
      assert request.member_id == ""
    end
  end
end
