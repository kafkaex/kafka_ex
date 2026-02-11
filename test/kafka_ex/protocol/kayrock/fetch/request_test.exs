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

  describe "Version comparison" do
    test "V0-V2 do not have max_bytes at request level" do
      v0 = Request.build_request(%Kayrock.Fetch.V0.Request{}, @base_opts)
      v1 = Request.build_request(%Kayrock.Fetch.V1.Request{}, @base_opts)
      v2 = Request.build_request(%Kayrock.Fetch.V2.Request{}, @base_opts)

      # V0-V2 don't have max_bytes field or it's nil
      assert is_nil(Map.get(v0, :max_bytes)) or v0.max_bytes == nil
      assert is_nil(Map.get(v1, :max_bytes)) or v1.max_bytes == nil
      assert is_nil(Map.get(v2, :max_bytes)) or v2.max_bytes == nil
    end

    test "V3+ have max_bytes at request level" do
      v3 = Request.build_request(%Kayrock.Fetch.V3.Request{}, @base_opts)
      v4 = Request.build_request(%Kayrock.Fetch.V4.Request{}, @base_opts)
      v5 = Request.build_request(%Kayrock.Fetch.V5.Request{}, @base_opts)

      assert v3.max_bytes == 1_000_000
      assert v4.max_bytes == 1_000_000
      assert v5.max_bytes == 1_000_000
    end

    test "V4+ have isolation_level" do
      v3 = Request.build_request(%Kayrock.Fetch.V3.Request{}, @base_opts)
      v4 = Request.build_request(%Kayrock.Fetch.V4.Request{}, @base_opts)
      v5 = Request.build_request(%Kayrock.Fetch.V5.Request{}, @base_opts)

      # V3 doesn't have isolation_level
      assert is_nil(Map.get(v3, :isolation_level)) or v3.isolation_level == nil
      # V4+ have isolation_level
      assert v4.isolation_level == 0
      assert v5.isolation_level == 0
    end

    test "V5+ have log_start_offset in partition data" do
      v4 = Request.build_request(%Kayrock.Fetch.V4.Request{}, @base_opts)
      v5 = Request.build_request(%Kayrock.Fetch.V5.Request{}, @base_opts)
      v7 = Request.build_request(%Kayrock.Fetch.V7.Request{}, @base_opts)

      [%{partitions: [p4]}] = v4.topics
      [%{partitions: [p5]}] = v5.topics
      [%{partitions: [p7]}] = v7.topics

      refute Map.has_key?(p4, :log_start_offset)
      assert p5.log_start_offset == 0
      assert p7.log_start_offset == 0
    end

    test "V7 has session fields" do
      v5 = Request.build_request(%Kayrock.Fetch.V5.Request{}, @base_opts)
      v7 = Request.build_request(%Kayrock.Fetch.V7.Request{}, @base_opts)

      # V5 doesn't have session fields
      assert is_nil(Map.get(v5, :session_id)) or v5.session_id == nil
      # V7 has session fields
      assert v7.session_id == 0
      assert v7.session_epoch == -1
      assert v7.forgotten_topics_data == []
    end
  end
end
