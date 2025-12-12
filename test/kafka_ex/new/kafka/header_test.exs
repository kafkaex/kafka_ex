defmodule KafkaEx.New.Kafka.HeaderTest do
  use ExUnit.Case, async: true

  alias KafkaEx.New.Kafka.Header

  describe "new/2" do
    test "creates header with key and value" do
      result = Header.new("content-type", "application/json")
      assert %Header{key: "content-type", value: "application/json"} = result
    end

    test "creates header with empty value" do
      result = Header.new("x-empty", "")
      assert result.key == "x-empty"
      assert result.value == ""
    end

    test "creates header with empty key" do
      result = Header.new("", "value")
      assert result.key == ""
    end

    test "creates header with binary value" do
      binary_value = <<1, 2, 3, 255, 254>>
      result = Header.new("binary-data", binary_value)
      assert result.value == binary_value
    end

    test "creates header with UTF-8 value" do
      result = Header.new("greeting", "Hello, \u4e16\u754c")
      assert result.value == "Hello, \u4e16\u754c"
    end

    test "raises FunctionClauseError for non-string key" do
      assert_raise FunctionClauseError, fn ->
        Header.new(:key, "value")
      end
    end

    test "raises FunctionClauseError for non-binary value" do
      assert_raise FunctionClauseError, fn ->
        Header.new("key", :value)
      end
    end
  end

  describe "build/1" do
    test "builds header from keyword opts" do
      result = Header.build(key: "x-custom", value: "data")
      assert %Header{key: "x-custom", value: "data"} = result
    end

    test "raises KeyError when key is missing" do
      assert_raise KeyError, ~r/key :key not found/, fn ->
        Header.build(value: "test")
      end
    end

    test "raises KeyError when value is missing" do
      assert_raise KeyError, ~r/key :value not found/, fn ->
        Header.build(key: "test")
      end
    end

    test "ignores extra options" do
      result = Header.build(key: "k", value: "v", extra: "ignored")
      assert result.key == "k"
      assert result.value == "v"
      refute Map.has_key?(result, :extra)
    end
  end

  describe "from_tuple/1" do
    test "creates header from tuple" do
      result = Header.from_tuple({"content-type", "text/plain"})
      assert %Header{key: "content-type", value: "text/plain"} = result
    end

    test "creates header from tuple with empty value" do
      result = Header.from_tuple({"key", ""})
      assert result.value == ""
    end

    test "raises FunctionClauseError for invalid tuple" do
      assert_raise FunctionClauseError, fn ->
        Header.from_tuple({:key, "value"})
      end
    end
  end

  describe "to_tuple/1" do
    test "converts header to tuple" do
      header = Header.new("trace-id", "abc123")
      assert Header.to_tuple(header) == {"trace-id", "abc123"}
    end

    test "converts header with empty value" do
      header = Header.new("empty", "")
      assert Header.to_tuple(header) == {"empty", ""}
    end
  end

  describe "key/1" do
    test "returns header key" do
      header = Header.new("my-key", "value")
      assert Header.key(header) == "my-key"
    end
  end

  describe "value/1" do
    test "returns header value" do
      header = Header.new("key", "my-value")
      assert Header.value(header) == "my-value"
    end

    test "returns binary value" do
      header = Header.new("key", <<0, 1, 2>>)
      assert Header.value(header) == <<0, 1, 2>>
    end
  end

  describe "list_to_tuples/1" do
    test "converts list of headers to tuples" do
      headers = [
        Header.new("a", "1"),
        Header.new("b", "2"),
        Header.new("c", "3")
      ]

      result = Header.list_to_tuples(headers)
      assert result == [{"a", "1"}, {"b", "2"}, {"c", "3"}]
    end

    test "converts empty list" do
      assert Header.list_to_tuples([]) == []
    end

    test "converts single header" do
      headers = [Header.new("single", "value")]
      assert Header.list_to_tuples(headers) == [{"single", "value"}]
    end
  end

  describe "list_from_tuples/1" do
    test "converts list of tuples to headers" do
      tuples = [{"a", "1"}, {"b", "2"}, {"c", "3"}]
      result = Header.list_from_tuples(tuples)

      assert length(result) == 3
      assert Enum.at(result, 0) == %Header{key: "a", value: "1"}
      assert Enum.at(result, 1) == %Header{key: "b", value: "2"}
      assert Enum.at(result, 2) == %Header{key: "c", value: "3"}
    end

    test "converts empty list" do
      assert Header.list_from_tuples([]) == []
    end

    test "converts single tuple" do
      result = Header.list_from_tuples([{"single", "value"}])
      assert result == [%Header{key: "single", value: "value"}]
    end
  end

  describe "round-trip conversion" do
    test "to_tuple -> from_tuple preserves data" do
      original = Header.new("x-custom", "test-value")
      tuple = Header.to_tuple(original)
      restored = Header.from_tuple(tuple)

      assert restored.key == original.key
      assert restored.value == original.value
    end

    test "from_tuple -> to_tuple preserves data" do
      original = {"content-type", "application/octet-stream"}
      header = Header.from_tuple(original)
      result = Header.to_tuple(header)

      assert result == original
    end

    test "list_to_tuples -> list_from_tuples preserves data" do
      original_headers = [
        Header.new("h1", "v1"),
        Header.new("h2", "v2")
      ]

      tuples = Header.list_to_tuples(original_headers)
      restored_headers = Header.list_from_tuples(tuples)

      assert Enum.map(restored_headers, & &1.key) == ["h1", "h2"]
      assert Enum.map(restored_headers, & &1.value) == ["v1", "v2"]
    end
  end

  describe "struct behavior" do
    test "can pattern match on struct" do
      header = Header.new("key", "value")
      assert %Header{key: "key", value: "value"} = header
    end

    test "can access fields directly" do
      header = Header.new("test-key", "test-value")
      assert header.key == "test-key"
      assert header.value == "test-value"
    end

    test "can update struct" do
      header = Header.new("key", "original")
      updated = %{header | value: "updated"}
      assert updated.value == "updated"
      assert updated.key == "key"
    end
  end

  describe "use cases" do
    test "tracing headers" do
      headers = [
        Header.new("trace-id", "abc123"),
        Header.new("span-id", "def456"),
        Header.new("parent-span-id", "ghi789")
      ]

      trace_id = Enum.find(headers, fn h -> h.key == "trace-id" end)
      assert trace_id.value == "abc123"
    end

    test "content type header" do
      header = Header.new("content-type", "application/json")
      assert header.key == "content-type"
      assert header.value == "application/json"
    end

    test "binary payload in header" do
      # Some use cases store serialized data in headers
      protobuf_like = <<10, 5, 104, 101, 108, 108, 111>>
      header = Header.new("schema", protobuf_like)
      assert header.value == protobuf_like
    end
  end
end
