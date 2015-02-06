defmodule Kafka.Protocol.Fetch.Test do
  use ExUnit.Case, async: true
  import Mock

  test "create_request creates a valid fetch request" do
    good_request = << 1 :: 16, 0 :: 16, 1 :: 32, 3 :: 16, "foo" :: binary, -1 :: 32, 10 :: 32, 1 :: 32, 1 :: 32, 3 :: 16, "bar" :: binary, 1 :: 32, 0 :: 32, 1 :: 64, 10000 :: 32 >>
    request = Kafka.Protocol.Fetch.create_request(1, "foo", "bar", 0, 1, 10, 1, 10000)
    assert request == good_request
  end

  test "parse_response correctly parses a valid response with a key and a value" do
    response = << 0 :: 32, 1 :: 32, 3 :: 16, "bar" :: binary, 1 :: 32, 0 :: 32, 0 :: 16, 10 :: 64, 32 :: 32, 1 :: 64, 20 :: 32, 0 :: 32, 0 :: 8, 0 :: 8,
      3 :: 32, "foo" :: binary, 3 :: 32, "bar" :: binary >>
    assert {:ok, 
      %{"bar" => %{0 => %{:error_code => 0, :hw_mark_offset => 10,
        :message_set => [%{:attributes => 0, :offset => 1, :crc => 0,
        :key => "foo", :value => "bar"}]}}
       }
    } = Kafka.Protocol.Fetch.parse_response(response)
  end

  test "parse_response correctly parses a valid response with a nil key and a value" do
    response = << 0 :: 32, 1 :: 32, 3 :: 16, "bar" :: binary, 1 :: 32, 0 :: 32, 0 :: 16, 10 :: 64, 29 :: 32, 1 :: 64, 17 :: 32, 0 :: 32, 0 :: 8, 0 :: 8,
      -1 :: 32, 3 :: 32, "bar" :: binary >>
    assert {:ok, 
      %{"bar" => %{0 => %{:error_code => 0, :hw_mark_offset => 10,
        :message_set => [%{:attributes => 0, :offset => 1, :crc => 0,
        :key => nil, :value => "bar"}]}}
       }
    } = Kafka.Protocol.Fetch.parse_response(response)
  end

  test "parse_response correctly parses a valid response with a key and a nil value" do
    response = << 0 :: 32, 1 :: 32, 3 :: 16, "bar" :: binary, 1 :: 32, 0 :: 32, 0 :: 16, 10 :: 64, 29 :: 32, 1 :: 64, 17 :: 32, 0 :: 32, 0 :: 8, 0 :: 8,
      3 :: 32, "foo" :: binary, -1 :: 32 >>
    assert {:ok, 
      %{"bar" => %{0 => %{:error_code => 0, :hw_mark_offset => 10,
        :message_set => [%{:attributes => 0, :offset => 1, :crc => 0,
        :key => "foo", :value => nil}]}}
       }
    } = Kafka.Protocol.Fetch.parse_response(response)
  end

  test "parse_response correctly parses a valid response with multiple messages" do
    response = << 0 :: 32, 1 :: 32, 3 :: 16, "bar" :: binary, 1 :: 32, 0 :: 32, 0 :: 16, 10 :: 64, 58 :: 32, 1 :: 64, 17 :: 32, 0 :: 32, 0 :: 8, 0 :: 8,
      -1 :: 32, 3 :: 32, "bar" :: binary, 2 :: 64, 17 :: 32, 0 :: 32, 0 :: 8, 0 :: 8, -1 :: 32, 3 :: 32, "baz" :: binary >>
    assert {:ok,
            %{"bar" => %{0 => %{:error_code => 0, :hw_mark_offset => 10, :message_set => [%{:attributes => 0, :crc => 0, :key => nil, :value => "bar"},%{:attributes => 0, :crc => 0, :key => nil, :value => "baz"}]}}},
            } = Kafka.Protocol.Fetch.parse_response(response)
  end

  test "parse_response correctly parses a valid response with multiple partitions" do
    response = << 0 :: 32, 1 :: 32, 3 :: 16, "bar" :: binary, 2 :: 32, 0 :: 32, 0 :: 16, 10 :: 64, 29 :: 32, 1 :: 64, 17 :: 32, 0 :: 32, 0 :: 8, 0 :: 8,
      -1 :: 32, 3 :: 32, "bar" :: binary, 1 :: 32, 0 :: 16, 10 :: 64, 29 :: 32, 1 :: 64, 17 :: 32, 0 :: 32, 0 :: 8, 0 :: 8, -1 :: 32, 3 :: 32, "baz" :: binary >>
    assert {:ok,
            %{"bar" => %{0 => %{:error_code => 0, :hw_mark_offset => 10, :message_set => [%{:attributes => 0, :crc => 0, :key => nil, :value => "bar"}]},
                         1 => %{:error_code => 0, :hw_mark_offset => 10, :message_set => [%{:attributes => 0, :crc => 0, :key => nil, :value => "baz"}]}}},
            } = Kafka.Protocol.Fetch.parse_response(response)
  end

  test "parse_response correctly parses a valid response with multiple topics" do
    response = << 0 :: 32, 2 :: 32,
      3 :: 16, "bar" :: binary, 1 :: 32, 0 :: 32, 0 :: 16, 10 :: 64, 29 :: 32, 1 :: 64, 17 :: 32, 0 :: 32, 0 :: 8, 0 :: 8, -1 :: 32, 3 :: 32, "foo" :: binary,
      3 :: 16, "baz" :: binary, 1 :: 32, 0 :: 32, 0 :: 16, 10 :: 64, 29 :: 32, 1 :: 64, 17 :: 32, 0 :: 32, 0 :: 8, 0 :: 8, -1 :: 32, 3 :: 32, "bar" :: binary >>
    assert {:ok,
            %{"bar" => %{0 => %{:error_code => 0, :hw_mark_offset => 10, :message_set => [%{:attributes => 0, :crc => 0, :key => nil, :value => "foo"}]}},
              "baz" => %{0 => %{:error_code => 0, :hw_mark_offset => 10, :message_set => [%{:attributes => 0, :crc => 0, :key => nil, :value => "bar"}]}}},
            } = Kafka.Protocol.Fetch.parse_response(response)
  end

  test "parse_response returns an error parsing an invalid response" do
    response = << 0 :: 32, -1 :: 32, 3 :: 16, "bar" :: binary, 1 :: 32, 0 :: 32, 0 :: 16, 10 :: 64, 32 :: 32, 1 :: 64, 20 :: 32, 0 :: 32, 0 :: 8, 0 :: 8,
      3 :: 32, "foo" :: binary, 3 :: 32, "bar" :: binary >>
    assert {:error, _, _} = Kafka.Protocol.Fetch.parse_response(response)
  end
end
