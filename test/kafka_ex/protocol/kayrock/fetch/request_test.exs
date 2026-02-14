defmodule KafkaEx.Protocol.Kayrock.Fetch.RequestTest do
  use ExUnit.Case, async: true

  alias KafkaEx.Protocol.Kayrock.Fetch.Request

  @base_opts [
    topic: "test-topic",
    partition: 0,
    offset: 100
  ]

  describe "V0 Request implementation" do
    test "builds V0 request with required fields" do
      template = %Kayrock.Fetch.V0.Request{}

      result = Request.build_request(template, @base_opts)

      assert result.replica_id == -1
      assert result.max_wait_time == 10_000
      assert result.min_bytes == 1
      assert [%{topic: "test-topic", partitions: [partition]}] = result.topics
      assert partition.partition == 0
      assert partition.fetch_offset == 100
      assert partition.partition_max_bytes == 1_000_000
    end

    test "builds V0 request with custom options" do
      template = %Kayrock.Fetch.V0.Request{}

      opts =
        @base_opts ++
          [
            max_bytes: 500_000,
            max_wait_time: 5_000,
            min_bytes: 100
          ]

      result = Request.build_request(template, opts)

      assert result.max_wait_time == 5_000
      assert result.min_bytes == 100
      assert [%{partitions: [partition]}] = result.topics
      assert partition.partition_max_bytes == 500_000
    end

    test "V0 request does not include log_start_offset" do
      template = %Kayrock.Fetch.V0.Request{}

      result = Request.build_request(template, @base_opts)

      assert [%{partitions: [partition]}] = result.topics
      refute Map.has_key?(partition, :log_start_offset)
    end
  end

  describe "V1 Request implementation" do
    test "builds V1 request (same structure as V0)" do
      template = %Kayrock.Fetch.V1.Request{}

      result = Request.build_request(template, @base_opts)

      assert result.replica_id == -1
      assert result.max_wait_time == 10_000
      assert result.min_bytes == 1
      assert [%{topic: "test-topic", partitions: [partition]}] = result.topics
      assert partition.partition == 0
      assert partition.fetch_offset == 100
    end
  end

  describe "V2 Request implementation" do
    test "builds V2 request (same structure as V0/V1)" do
      template = %Kayrock.Fetch.V2.Request{}

      result = Request.build_request(template, @base_opts)

      assert result.replica_id == -1
      assert result.max_wait_time == 10_000
      assert [%{topic: "test-topic", partitions: [partition]}] = result.topics
      assert partition.fetch_offset == 100
    end
  end

  describe "V3 Request implementation" do
    test "builds V3 request with max_bytes at request level" do
      template = %Kayrock.Fetch.V3.Request{}

      result = Request.build_request(template, @base_opts)

      assert result.replica_id == -1
      assert result.max_wait_time == 10_000
      assert result.min_bytes == 1
      # V3 adds max_bytes at request level
      assert result.max_bytes == 1_000_000
      assert [%{topic: "test-topic", partitions: [partition]}] = result.topics
      assert partition.fetch_offset == 100
    end

    test "allows custom max_bytes" do
      template = %Kayrock.Fetch.V3.Request{}

      opts = @base_opts ++ [max_bytes: 2_000_000]

      result = Request.build_request(template, opts)

      assert result.max_bytes == 2_000_000
    end
  end

  describe "V4 Request implementation" do
    test "builds V4 request with isolation_level" do
      template = %Kayrock.Fetch.V4.Request{}

      result = Request.build_request(template, @base_opts)

      assert result.replica_id == -1
      assert result.max_wait_time == 10_000
      assert result.min_bytes == 1
      assert result.max_bytes == 1_000_000
      # V4 adds isolation_level (default 0 = READ_UNCOMMITTED)
      assert result.isolation_level == 0
    end

    test "allows READ_COMMITTED isolation level" do
      template = %Kayrock.Fetch.V4.Request{}

      opts = @base_opts ++ [isolation_level: 1]

      result = Request.build_request(template, opts)

      assert result.isolation_level == 1
    end

    test "V4 request does not include log_start_offset" do
      template = %Kayrock.Fetch.V4.Request{}

      result = Request.build_request(template, @base_opts)

      assert [%{partitions: [partition]}] = result.topics
      refute Map.has_key?(partition, :log_start_offset)
    end
  end

  describe "V5 Request implementation" do
    test "builds V5 request with log_start_offset in partition" do
      template = %Kayrock.Fetch.V5.Request{}

      result = Request.build_request(template, @base_opts)

      assert result.replica_id == -1
      assert result.max_wait_time == 10_000
      assert result.min_bytes == 1
      assert result.max_bytes == 1_000_000
      assert result.isolation_level == 0
      # V5 adds log_start_offset in partition data
      assert [%{partitions: [partition]}] = result.topics
      assert partition.log_start_offset == 0
    end

    test "allows custom log_start_offset" do
      template = %Kayrock.Fetch.V5.Request{}

      opts = @base_opts ++ [log_start_offset: 50]

      result = Request.build_request(template, opts)

      assert [%{partitions: [partition]}] = result.topics
      assert partition.log_start_offset == 50
    end
  end

  describe "V6 Request implementation" do
    test "builds V6 request (same structure as V5)" do
      template = %Kayrock.Fetch.V6.Request{}

      result = Request.build_request(template, @base_opts)

      assert result.replica_id == -1
      assert result.max_bytes == 1_000_000
      assert result.isolation_level == 0
      assert [%{partitions: [partition]}] = result.topics
      assert partition.log_start_offset == 0
    end
  end

  describe "V7 Request implementation" do
    test "builds V7 request with session fields" do
      template = %Kayrock.Fetch.V7.Request{}

      result = Request.build_request(template, @base_opts)

      assert result.replica_id == -1
      assert result.max_wait_time == 10_000
      assert result.min_bytes == 1
      assert result.max_bytes == 1_000_000
      assert result.isolation_level == 0
      # V7 adds session fields for incremental fetch
      assert result.session_id == 0
      assert result.session_epoch == -1
      assert result.forgotten_topics_data == []
      assert [%{partitions: [partition]}] = result.topics
      assert partition.log_start_offset == 0
    end

    test "allows custom session fields" do
      template = %Kayrock.Fetch.V7.Request{}

      opts =
        @base_opts ++
          [
            session_id: 123,
            epoch: 5,
            forgotten_topics_data: [%{topic: "old_topic"}]
          ]

      result = Request.build_request(template, opts)

      assert result.session_id == 123
      assert result.session_epoch == 5
      assert result.forgotten_topics_data == [%{topic: "old_topic"}]
    end

    test "builds V7 request with all custom options" do
      template = %Kayrock.Fetch.V7.Request{}

      opts = [
        topic: "events",
        partition: 2,
        offset: 500,
        max_bytes: 2_000_000,
        max_wait_time: 15_000,
        min_bytes: 500,
        isolation_level: 1,
        log_start_offset: 100,
        session_id: 42,
        epoch: 3
      ]

      result = Request.build_request(template, opts)

      assert result.max_bytes == 2_000_000
      assert result.max_wait_time == 15_000
      assert result.min_bytes == 500
      assert result.isolation_level == 1
      assert result.session_id == 42
      assert result.session_epoch == 3
      assert [%{topic: "events", partitions: [partition]}] = result.topics
      assert partition.partition == 2
      assert partition.fetch_offset == 500
      assert partition.log_start_offset == 100
    end
  end

  describe "V8 Request implementation" do
    test "builds V8 request with session fields (same as V7)" do
      template = %Kayrock.Fetch.V8.Request{}

      result = Request.build_request(template, @base_opts)

      assert result.replica_id == -1
      assert result.max_wait_time == 10_000
      assert result.min_bytes == 1
      assert result.max_bytes == 1_000_000
      assert result.isolation_level == 0
      assert result.session_id == 0
      assert result.session_epoch == -1
      assert result.forgotten_topics_data == []
      assert [%{topic: "test-topic", partitions: [partition]}] = result.topics
      assert partition.log_start_offset == 0
    end

    test "allows custom session fields" do
      template = %Kayrock.Fetch.V8.Request{}

      opts =
        @base_opts ++
          [
            session_id: 200,
            epoch: 10,
            forgotten_topics_data: [%{topic: "forgotten"}]
          ]

      result = Request.build_request(template, opts)

      assert result.session_id == 200
      assert result.session_epoch == 10
      assert result.forgotten_topics_data == [%{topic: "forgotten"}]
    end
  end

  describe "V9 Request implementation" do
    test "builds V9 request with current_leader_epoch in partitions" do
      template = %Kayrock.Fetch.V9.Request{}

      result = Request.build_request(template, @base_opts)

      assert result.replica_id == -1
      assert result.max_wait_time == 10_000
      assert result.min_bytes == 1
      assert result.max_bytes == 1_000_000
      assert result.isolation_level == 0
      assert result.session_id == 0
      assert result.session_epoch == -1
      assert [%{topic: "test-topic", partitions: [partition]}] = result.topics
      assert partition.log_start_offset == 0
      # V9 adds current_leader_epoch
      assert partition.current_leader_epoch == -1
    end

    test "allows custom current_leader_epoch" do
      template = %Kayrock.Fetch.V9.Request{}

      opts = @base_opts ++ [current_leader_epoch: 7]

      result = Request.build_request(template, opts)

      assert [%{partitions: [partition]}] = result.topics
      assert partition.current_leader_epoch == 7
    end

    test "builds V9 request with all custom options" do
      template = %Kayrock.Fetch.V9.Request{}

      opts = [
        topic: "events",
        partition: 3,
        offset: 1000,
        max_bytes: 2_000_000,
        max_wait_time: 15_000,
        min_bytes: 500,
        isolation_level: 1,
        log_start_offset: 200,
        session_id: 55,
        epoch: 8,
        current_leader_epoch: 12
      ]

      result = Request.build_request(template, opts)

      assert result.max_bytes == 2_000_000
      assert result.max_wait_time == 15_000
      assert result.min_bytes == 500
      assert result.isolation_level == 1
      assert result.session_id == 55
      assert result.session_epoch == 8
      assert [%{topic: "events", partitions: [partition]}] = result.topics
      assert partition.partition == 3
      assert partition.fetch_offset == 1000
      assert partition.log_start_offset == 200
      assert partition.current_leader_epoch == 12
    end
  end

  describe "V10 Request implementation" do
    test "builds V10 request (same as V9)" do
      template = %Kayrock.Fetch.V10.Request{}

      result = Request.build_request(template, @base_opts)

      assert result.replica_id == -1
      assert result.max_bytes == 1_000_000
      assert result.isolation_level == 0
      assert result.session_id == 0
      assert [%{partitions: [partition]}] = result.topics
      assert partition.current_leader_epoch == -1
      assert partition.log_start_offset == 0
    end

    test "allows custom current_leader_epoch" do
      template = %Kayrock.Fetch.V10.Request{}

      opts = @base_opts ++ [current_leader_epoch: 15]

      result = Request.build_request(template, opts)

      assert [%{partitions: [partition]}] = result.topics
      assert partition.current_leader_epoch == 15
    end
  end

  describe "V11 Request implementation" do
    test "builds V11 request with rack_id" do
      template = %Kayrock.Fetch.V11.Request{}

      result = Request.build_request(template, @base_opts)

      assert result.replica_id == -1
      assert result.max_wait_time == 10_000
      assert result.min_bytes == 1
      assert result.max_bytes == 1_000_000
      assert result.isolation_level == 0
      assert result.session_id == 0
      assert result.session_epoch == -1
      # V11 adds rack_id
      assert result.rack_id == ""
      assert [%{topic: "test-topic", partitions: [partition]}] = result.topics
      assert partition.log_start_offset == 0
      assert partition.current_leader_epoch == -1
    end

    test "allows custom rack_id" do
      template = %Kayrock.Fetch.V11.Request{}

      opts = @base_opts ++ [rack_id: "us-east-1a"]

      result = Request.build_request(template, opts)

      assert result.rack_id == "us-east-1a"
    end

    test "builds V11 request with all custom options" do
      template = %Kayrock.Fetch.V11.Request{}

      opts = [
        topic: "events",
        partition: 5,
        offset: 2000,
        max_bytes: 3_000_000,
        max_wait_time: 20_000,
        min_bytes: 1000,
        isolation_level: 1,
        log_start_offset: 500,
        session_id: 99,
        epoch: 15,
        current_leader_epoch: 20,
        rack_id: "eu-west-1b"
      ]

      result = Request.build_request(template, opts)

      assert result.max_bytes == 3_000_000
      assert result.max_wait_time == 20_000
      assert result.min_bytes == 1000
      assert result.isolation_level == 1
      assert result.session_id == 99
      assert result.session_epoch == 15
      assert result.rack_id == "eu-west-1b"
      assert [%{topic: "events", partitions: [partition]}] = result.topics
      assert partition.partition == 5
      assert partition.fetch_offset == 2000
      assert partition.log_start_offset == 500
      assert partition.current_leader_epoch == 20
    end
  end

  describe "struct field inventory across versions" do
    test "V0-V2 request structs lack max_bytes, isolation_level, session_id" do
      for version <- [0, 1, 2] do
        struct = Kayrock.Fetch.get_request_struct(version)

        refute Map.has_key?(struct, :isolation_level),
               "V#{version} should not have isolation_level"

        refute Map.has_key?(struct, :session_id),
               "V#{version} should not have session_id"
      end
    end

    test "V3 adds max_bytes but lacks isolation_level" do
      struct = Kayrock.Fetch.get_request_struct(3)

      assert Map.has_key?(struct, :max_bytes), "V3 should have max_bytes"

      refute Map.has_key?(struct, :isolation_level),
             "V3 should not have isolation_level"
    end

    test "V4+ have isolation_level" do
      for version <- 4..11 do
        struct = Kayrock.Fetch.get_request_struct(version)

        assert Map.has_key?(struct, :isolation_level),
               "V#{version} should have isolation_level"
      end
    end

    test "V7+ have session_id and session_epoch" do
      for version <- 7..11 do
        struct = Kayrock.Fetch.get_request_struct(version)

        assert Map.has_key?(struct, :session_id),
               "V#{version} should have session_id"

        assert Map.has_key?(struct, :session_epoch),
               "V#{version} should have session_epoch"
      end
    end

    test "V0-V8 lack current_leader_epoch in schema (struct level only has top fields)" do
      # Note: current_leader_epoch is inside partition sub-structs, not at
      # top level. We verify by checking the schema.
      for version <- 0..8 do
        schema = Kayrock.Fetch.get_request_struct(version).__struct__.schema()
        schema_str = inspect(schema)

        refute String.contains?(schema_str, "current_leader_epoch"),
               "V#{version} schema should not contain current_leader_epoch"
      end
    end

    test "V9+ schema includes current_leader_epoch in partition data" do
      for version <- 9..11 do
        schema = Kayrock.Fetch.get_request_struct(version).__struct__.schema()
        schema_str = inspect(schema)

        assert String.contains?(schema_str, "current_leader_epoch"),
               "V#{version} schema should contain current_leader_epoch"
      end
    end

    test "only V11 has rack_id" do
      for version <- 0..10 do
        struct = Kayrock.Fetch.get_request_struct(version)

        refute Map.has_key?(struct, :rack_id),
               "V#{version} should not have rack_id"
      end

      struct = Kayrock.Fetch.get_request_struct(11)
      assert Map.has_key?(struct, :rack_id), "V11 should have rack_id"
    end

    test "all versions have core fields: replica_id, max_wait_time, min_bytes, topics" do
      for version <- 0..11 do
        struct = Kayrock.Fetch.get_request_struct(version)
        assert Map.has_key?(struct, :replica_id), "V#{version} should have replica_id"
        assert Map.has_key?(struct, :max_wait_time), "V#{version} should have max_wait_time"
        assert Map.has_key?(struct, :min_bytes), "V#{version} should have min_bytes"
        assert Map.has_key?(struct, :topics), "V#{version} should have topics"
      end
    end
  end

  describe "Any fallback request implementation (forward compatibility)" do
    defmodule FakeV12SessionRequest do
      defstruct [
        :replica_id,
        :max_wait_time,
        :min_bytes,
        :max_bytes,
        :isolation_level,
        :session_id,
        :session_epoch,
        :topics,
        :forgotten_topics_data,
        :new_future_field
      ]
    end

    defmodule FakeV12RackRequest do
      defstruct [
        :replica_id,
        :max_wait_time,
        :min_bytes,
        :max_bytes,
        :isolation_level,
        :session_id,
        :session_epoch,
        :topics,
        :forgotten_topics_data,
        :rack_id,
        :new_future_field
      ]
    end

    defmodule FakePreV3Request do
      defstruct [:replica_id, :max_wait_time, :min_bytes, :topics]
    end

    test "Any fallback uses V7+ path when struct has :session_id" do
      template = %FakeV12SessionRequest{}

      result = Request.build_request(template, @base_opts)

      assert result.replica_id == -1
      assert result.max_bytes == 1_000_000
      assert result.isolation_level == 0
      assert result.session_id == 0
      assert result.session_epoch == -1
      assert [%{topic: "test-topic", partitions: [_]}] = result.topics
    end

    test "Any fallback uses V11 path when struct has :rack_id" do
      template = %FakeV12RackRequest{}

      opts = @base_opts ++ [rack_id: "rack-42"]

      result = Request.build_request(template, opts)

      assert result.rack_id == "rack-42"
      assert result.session_id == 0
      assert [%{partitions: [p]}] = result.topics
      # V11 path includes current_leader_epoch
      assert p.current_leader_epoch == -1
    end

    test "Any fallback uses V0-V2 path when struct lacks max_bytes and session_id" do
      template = %FakePreV3Request{}

      result = Request.build_request(template, @base_opts)

      assert result.replica_id == -1
      assert result.max_wait_time == 10_000
      assert result.min_bytes == 1
      assert [%{topic: "test-topic", partitions: [p]}] = result.topics
      assert p.fetch_offset == 100
      refute Map.has_key?(p, :log_start_offset)
    end
  end

  describe "build_request via KayrockProtocol" do
    alias KafkaEx.Protocol.KayrockProtocol

    for version <- 0..11 do
      test "dispatches V#{version} request correctly" do
        result =
          KayrockProtocol.build_request(:fetch, unquote(version), @base_opts)

        expected_struct = Kayrock.Fetch.get_request_struct(unquote(version))
        assert result.__struct__ == expected_struct.__struct__
      end
    end
  end

  describe "Version comparison" do
    test "V0-V2 do not have max_bytes at request level" do
      v0 = Request.build_request(%Kayrock.Fetch.V0.Request{}, @base_opts)
      v1 = Request.build_request(%Kayrock.Fetch.V1.Request{}, @base_opts)
      v2 = Request.build_request(%Kayrock.Fetch.V2.Request{}, @base_opts)

      assert is_nil(Map.get(v0, :max_bytes)) or v0.max_bytes == nil
      assert is_nil(Map.get(v1, :max_bytes)) or v1.max_bytes == nil
      assert is_nil(Map.get(v2, :max_bytes)) or v2.max_bytes == nil
    end

    test "V3+ have max_bytes at request level" do
      for version <- 3..11 do
        struct = Kayrock.Fetch.get_request_struct(version)
        result = Request.build_request(struct, @base_opts)
        assert result.max_bytes == 1_000_000, "V#{version} should have max_bytes == 1_000_000"
      end
    end

    test "V4+ have isolation_level" do
      v3 = Request.build_request(%Kayrock.Fetch.V3.Request{}, @base_opts)

      assert is_nil(Map.get(v3, :isolation_level)) or v3.isolation_level == nil

      for version <- 4..11 do
        struct = Kayrock.Fetch.get_request_struct(version)
        result = Request.build_request(struct, @base_opts)
        assert result.isolation_level == 0, "V#{version} should have isolation_level == 0"
      end
    end

    test "V5+ have log_start_offset in partition data" do
      v4 = Request.build_request(%Kayrock.Fetch.V4.Request{}, @base_opts)
      [%{partitions: [p4]}] = v4.topics
      refute Map.has_key?(p4, :log_start_offset)

      for version <- 5..11 do
        struct = Kayrock.Fetch.get_request_struct(version)
        result = Request.build_request(struct, @base_opts)
        [%{partitions: [p]}] = result.topics

        assert p.log_start_offset == 0,
               "V#{version} should have log_start_offset == 0"
      end
    end

    test "V7+ have session fields" do
      v5 = Request.build_request(%Kayrock.Fetch.V5.Request{}, @base_opts)
      assert is_nil(Map.get(v5, :session_id)) or v5.session_id == nil

      for version <- 7..11 do
        struct = Kayrock.Fetch.get_request_struct(version)
        result = Request.build_request(struct, @base_opts)
        assert result.session_id == 0, "V#{version} should have session_id == 0"
        assert result.session_epoch == -1, "V#{version} should have session_epoch == -1"
        assert result.forgotten_topics_data == [], "V#{version} should have empty forgotten_topics_data"
      end
    end

    test "V9+ have current_leader_epoch in partition data" do
      for version <- 7..8 do
        struct = Kayrock.Fetch.get_request_struct(version)
        result = Request.build_request(struct, @base_opts)
        [%{partitions: [p]}] = result.topics
        refute Map.has_key?(p, :current_leader_epoch), "V#{version} should not have current_leader_epoch"
      end

      for version <- 9..11 do
        struct = Kayrock.Fetch.get_request_struct(version)
        result = Request.build_request(struct, @base_opts)
        [%{partitions: [p]}] = result.topics
        assert p.current_leader_epoch == -1, "V#{version} should have current_leader_epoch == -1"
      end
    end

    test "only V11 has rack_id" do
      for version <- 0..10 do
        struct = Kayrock.Fetch.get_request_struct(version)
        result = Request.build_request(struct, @base_opts)
        assert !Map.has_key?(result, :rack_id) or result.rack_id == nil, "V#{version} should not have rack_id set"
      end

      result = Request.build_request(%Kayrock.Fetch.V11.Request{}, @base_opts)
      assert result.rack_id == ""
    end
  end
end
