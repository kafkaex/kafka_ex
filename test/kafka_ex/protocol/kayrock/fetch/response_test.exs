defmodule KafkaEx.Protocol.Kayrock.Fetch.ResponseTest do
  use ExUnit.Case, async: true

  alias KafkaEx.Protocol.Kayrock.Fetch.Response
  alias KafkaEx.Messages.Fetch
  alias KafkaEx.Messages.Header

  # Helper to build a basic successful response structure
  defp build_response(topic, partition, error_code, high_watermark, messages, extra_fields \\ %{}) do
    partition_header =
      %{
        partition: partition,
        error_code: error_code,
        high_watermark: high_watermark
      }
      |> Map.merge(Map.get(extra_fields, :partition_header, %{}))

    base = %{
      responses: [
        %{
          topic: topic,
          partition_responses: [
            %{
              partition_header: partition_header,
              record_set: messages
            }
          ]
        }
      ]
    }

    Map.merge(base, Map.drop(extra_fields, [:partition_header]))
  end

  defp build_message_set(messages) do
    %Kayrock.MessageSet{
      messages:
        Enum.map(messages, fn msg ->
          %Kayrock.MessageSet.Message{
            offset: msg[:offset] || 0,
            key: msg[:key],
            value: msg[:value],
            timestamp: msg[:timestamp] || 0,
            attributes: msg[:attributes] || 0,
            crc: msg[:crc] || 12345
          }
        end)
    }
  end

  defp build_record_batch(records, batch_attributes \\ 0) do
    [
      %Kayrock.RecordBatch{
        attributes: batch_attributes,
        records:
          Enum.map(records, fn rec ->
            %Kayrock.RecordBatch.Record{
              offset: rec[:offset] || 0,
              key: rec[:key],
              value: rec[:value],
              timestamp: rec[:timestamp] || 0,
              attributes: rec[:attributes] || 0,
              headers: build_headers(rec[:headers])
            }
          end)
      }
    ]
  end

  defp build_headers(nil), do: nil
  defp build_headers([]), do: []

  defp build_headers(headers) do
    Enum.map(headers, fn {k, v} ->
      %Kayrock.RecordBatch.RecordHeader{key: k, value: v}
    end)
  end

  describe "V0 Response implementation" do
    test "parses successful response with MessageSet" do
      message_set = build_message_set([%{offset: 0, key: "k1", value: "v1"}])

      response =
        build_response("test-topic", 0, 0, 100, message_set)
        |> Map.put(:__struct__, Kayrock.Fetch.V0.Response)

      assert {:ok, %Fetch{} = fetch} = Response.parse_response(response)

      assert fetch.topic == "test-topic"
      assert fetch.partition == 0
      assert fetch.high_watermark == 100
      assert length(fetch.records) == 1
      assert fetch.last_offset == 0
      # V0 has no throttle_time_ms
      assert is_nil(fetch.throttle_time_ms) or fetch.throttle_time_ms == nil
    end

    test "parses response with multiple messages" do
      message_set =
        build_message_set([
          %{offset: 0, key: "k1", value: "v1"},
          %{offset: 1, key: "k2", value: "v2"},
          %{offset: 2, key: "k3", value: "v3"}
        ])

      response =
        build_response("events", 1, 0, 500, message_set)
        |> Map.put(:__struct__, Kayrock.Fetch.V0.Response)

      assert {:ok, fetch} = Response.parse_response(response)

      assert fetch.topic == "events"
      assert fetch.partition == 1
      assert fetch.high_watermark == 500
      assert length(fetch.records) == 3
      assert fetch.last_offset == 2
    end

    test "parses response with error code" do
      response =
        build_response("test-topic", 0, 1, 0, nil)
        |> Map.put(:__struct__, Kayrock.Fetch.V0.Response)

      assert {:error, error} = Response.parse_response(response)
      assert error.error == :offset_out_of_range
    end

    test "parses empty response" do
      response =
        %{responses: []}
        |> Map.put(:__struct__, Kayrock.Fetch.V0.Response)

      assert {:error, error} = Response.parse_response(response)
      assert error.error == :empty_response
    end

    test "parses response with nil record_set" do
      response =
        build_response("test-topic", 0, 0, 100, nil)
        |> Map.put(:__struct__, Kayrock.Fetch.V0.Response)

      assert {:ok, fetch} = Response.parse_response(response)

      assert fetch.records == []
      assert fetch.last_offset == nil
    end
  end

  describe "V1 Response implementation" do
    test "parses response with throttle_time_ms" do
      message_set = build_message_set([%{offset: 5, value: "test"}])

      response =
        build_response("test-topic", 0, 0, 100, message_set, %{throttle_time_ms: 50})
        |> Map.put(:__struct__, Kayrock.Fetch.V1.Response)

      assert {:ok, fetch} = Response.parse_response(response)

      assert fetch.topic == "test-topic"
      assert fetch.throttle_time_ms == 50
      assert fetch.last_offset == 5
    end

    test "defaults throttle_time_ms to 0 when not present" do
      message_set = build_message_set([%{offset: 0, value: "test"}])

      response =
        build_response("test-topic", 0, 0, 100, message_set)
        |> Map.put(:__struct__, Kayrock.Fetch.V1.Response)

      assert {:ok, fetch} = Response.parse_response(response)

      assert fetch.throttle_time_ms == 0
    end
  end

  describe "V2 Response implementation" do
    test "parses response with throttle_time_ms (same as V1)" do
      message_set = build_message_set([%{offset: 10, value: "v2-test"}])

      response =
        build_response("v2-topic", 2, 0, 200, message_set, %{throttle_time_ms: 100})
        |> Map.put(:__struct__, Kayrock.Fetch.V2.Response)

      assert {:ok, fetch} = Response.parse_response(response)

      assert fetch.topic == "v2-topic"
      assert fetch.partition == 2
      assert fetch.throttle_time_ms == 100
    end
  end

  describe "V3 Response implementation" do
    test "parses response with throttle_time_ms (same as V1/V2)" do
      message_set = build_message_set([%{offset: 15, value: "v3-test"}])

      response =
        build_response("v3-topic", 0, 0, 300, message_set, %{throttle_time_ms: 25})
        |> Map.put(:__struct__, Kayrock.Fetch.V3.Response)

      assert {:ok, fetch} = Response.parse_response(response)

      assert fetch.throttle_time_ms == 25
    end
  end

  describe "V4 Response implementation" do
    test "parses response with last_stable_offset and aborted_transactions" do
      record_batch = build_record_batch([%{offset: 20, value: "v4-test"}])

      response =
        build_response("v4-topic", 0, 0, 400, record_batch, %{
          throttle_time_ms: 10,
          partition_header: %{
            last_stable_offset: 350,
            aborted_transactions: []
          }
        })
        |> Map.put(:__struct__, Kayrock.Fetch.V4.Response)

      assert {:ok, fetch} = Response.parse_response(response)

      assert fetch.topic == "v4-topic"
      assert fetch.throttle_time_ms == 10
      assert fetch.last_stable_offset == 350
      assert fetch.aborted_transactions == []
    end

    test "parses response with aborted transactions" do
      record_batch = build_record_batch([%{offset: 25, value: "tx-data"}])

      aborted_txns = [
        %{producer_id: 123, first_offset: 10},
        %{producer_id: 456, first_offset: 15}
      ]

      response =
        build_response("tx-topic", 0, 0, 500, record_batch, %{
          throttle_time_ms: 0,
          partition_header: %{
            last_stable_offset: 400,
            aborted_transactions: aborted_txns
          }
        })
        |> Map.put(:__struct__, Kayrock.Fetch.V4.Response)

      assert {:ok, fetch} = Response.parse_response(response)

      assert length(fetch.aborted_transactions) == 2
    end

    test "parses RecordBatch with headers" do
      record_batch =
        build_record_batch([
          %{
            offset: 30,
            value: "with-headers",
            headers: [{"content-type", "text/plain"}, {"trace-id", "abc123"}]
          }
        ])

      response =
        build_response("headers-topic", 0, 0, 100, record_batch, %{
          partition_header: %{last_stable_offset: 90}
        })
        |> Map.put(:__struct__, Kayrock.Fetch.V4.Response)

      assert {:ok, fetch} = Response.parse_response(response)

      [record] = fetch.records

      assert record.headers == [
               %Header{key: "content-type", value: "text/plain"},
               %Header{key: "trace-id", value: "abc123"}
             ]
    end
  end

  describe "V5 Response implementation" do
    test "parses response with log_start_offset" do
      record_batch = build_record_batch([%{offset: 50, value: "v5-test"}])

      response =
        build_response("v5-topic", 0, 0, 600, record_batch, %{
          throttle_time_ms: 5,
          partition_header: %{
            last_stable_offset: 550,
            log_start_offset: 100,
            aborted_transactions: nil
          }
        })
        |> Map.put(:__struct__, Kayrock.Fetch.V5.Response)

      assert {:ok, fetch} = Response.parse_response(response)

      assert fetch.topic == "v5-topic"
      assert fetch.throttle_time_ms == 5
      assert fetch.last_stable_offset == 550
      assert fetch.log_start_offset == 100
    end
  end

  describe "V6 Response implementation" do
    test "parses response (same fields as V5)" do
      record_batch = build_record_batch([%{offset: 60, value: "v6-test"}])

      response =
        build_response("v6-topic", 1, 0, 700, record_batch, %{
          throttle_time_ms: 15,
          partition_header: %{
            last_stable_offset: 650,
            log_start_offset: 200,
            aborted_transactions: []
          }
        })
        |> Map.put(:__struct__, Kayrock.Fetch.V6.Response)

      assert {:ok, fetch} = Response.parse_response(response)

      assert fetch.topic == "v6-topic"
      assert fetch.partition == 1
      assert fetch.log_start_offset == 200
    end
  end

  describe "V7 Response implementation" do
    test "parses response (same fields as V5/V6)" do
      record_batch = build_record_batch([%{offset: 70, value: "v7-test"}])

      response =
        build_response("v7-topic", 2, 0, 800, record_batch, %{
          throttle_time_ms: 20,
          partition_header: %{
            last_stable_offset: 750,
            log_start_offset: 300,
            aborted_transactions: nil
          }
        })
        |> Map.put(:__struct__, Kayrock.Fetch.V7.Response)

      assert {:ok, fetch} = Response.parse_response(response)

      assert fetch.topic == "v7-topic"
      assert fetch.partition == 2
      assert fetch.high_watermark == 800
      assert fetch.throttle_time_ms == 20
      assert fetch.last_stable_offset == 750
      assert fetch.log_start_offset == 300
    end

    test "parses response with full RecordBatch data" do
      record_batch =
        build_record_batch([
          %{offset: 100, key: "k1", value: "v1", timestamp: 1000},
          %{offset: 101, key: "k2", value: "v2", timestamp: 1001, headers: [{"h", "v"}]}
        ])

      response =
        build_response("full-topic", 0, 0, 1000, record_batch, %{
          throttle_time_ms: 0,
          partition_header: %{
            last_stable_offset: 999,
            log_start_offset: 50,
            aborted_transactions: []
          }
        })
        |> Map.put(:__struct__, Kayrock.Fetch.V7.Response)

      assert {:ok, fetch} = Response.parse_response(response)

      assert length(fetch.records) == 2
      assert fetch.last_offset == 101

      [r1, r2] = fetch.records
      assert r1.key == "k1"
      assert r1.value == "v1"
      assert r2.headers == [%Header{key: "h", value: "v"}]
    end
  end

  describe "Error handling" do
    test "returns error for unknown_topic_or_partition" do
      response =
        build_response("missing-topic", 0, 3, 0, nil)
        |> Map.put(:__struct__, Kayrock.Fetch.V0.Response)

      assert {:error, error} = Response.parse_response(response)
      assert error.error == :unknown_topic_or_partition
    end

    test "returns error for leader_not_available" do
      response =
        build_response("test-topic", 0, 5, 0, nil)
        |> Map.put(:__struct__, Kayrock.Fetch.V1.Response)

      assert {:error, error} = Response.parse_response(response)
      assert error.error == :leader_not_available
    end

    test "returns error for not_leader_for_partition" do
      response =
        build_response("test-topic", 0, 6, 0, nil)
        |> Map.put(:__struct__, Kayrock.Fetch.V4.Response)

      assert {:error, error} = Response.parse_response(response)
      assert error.error == :not_leader_for_partition
    end
  end

  describe "Timestamp type extraction" do
    test "extracts create_time from MessageSet attributes (bit 3 = 0)" do
      message_set = build_message_set([%{offset: 0, value: "test", attributes: 0}])

      response =
        build_response("test-topic", 0, 0, 100, message_set)
        |> Map.put(:__struct__, Kayrock.Fetch.V0.Response)

      assert {:ok, fetch} = Response.parse_response(response)

      [record] = fetch.records
      assert record.timestamp_type == :create_time
    end

    test "extracts log_append_time from MessageSet attributes (bit 3 = 1)" do
      # attributes = 8 means bit 3 is set (0b1000)
      message_set = build_message_set([%{offset: 0, value: "test", attributes: 8}])

      response =
        build_response("test-topic", 0, 0, 100, message_set)
        |> Map.put(:__struct__, Kayrock.Fetch.V1.Response)

      assert {:ok, fetch} = Response.parse_response(response)

      [record] = fetch.records
      assert record.timestamp_type == :log_append_time
    end

    test "extracts timestamp_type from RecordBatch attributes" do
      # Batch with LogAppendTime (attributes bit 3 = 1)
      record_batch = build_record_batch([%{offset: 0, value: "test"}], 8)

      response =
        build_response("test-topic", 0, 0, 100, record_batch, %{
          partition_header: %{last_stable_offset: 90}
        })
        |> Map.put(:__struct__, Kayrock.Fetch.V4.Response)

      assert {:ok, fetch} = Response.parse_response(response)

      [record] = fetch.records
      assert record.timestamp_type == :log_append_time
    end
  end

  describe "Version comparison" do
    test "V0 has no throttle_time_ms" do
      message_set = build_message_set([%{offset: 0, value: "test"}])

      v0_response =
        build_response("test-topic", 0, 0, 100, message_set)
        |> Map.put(:__struct__, Kayrock.Fetch.V0.Response)

      assert {:ok, fetch} = Response.parse_response(v0_response)
      assert is_nil(fetch.throttle_time_ms) or fetch.throttle_time_ms == nil
    end

    test "V1+ have throttle_time_ms" do
      message_set = build_message_set([%{offset: 0, value: "test"}])

      v1_response =
        build_response("test-topic", 0, 0, 100, message_set, %{throttle_time_ms: 10})
        |> Map.put(:__struct__, Kayrock.Fetch.V1.Response)

      assert {:ok, fetch} = Response.parse_response(v1_response)
      assert fetch.throttle_time_ms == 10
    end

    test "V4+ have last_stable_offset" do
      record_batch = build_record_batch([%{offset: 0, value: "test"}])

      v4_response =
        build_response("test-topic", 0, 0, 100, record_batch, %{
          partition_header: %{last_stable_offset: 90}
        })
        |> Map.put(:__struct__, Kayrock.Fetch.V4.Response)

      assert {:ok, fetch} = Response.parse_response(v4_response)
      assert fetch.last_stable_offset == 90
    end

    test "V5+ have log_start_offset" do
      record_batch = build_record_batch([%{offset: 0, value: "test"}])

      v5_response =
        build_response("test-topic", 0, 0, 100, record_batch, %{
          partition_header: %{log_start_offset: 50, last_stable_offset: 90}
        })
        |> Map.put(:__struct__, Kayrock.Fetch.V5.Response)

      assert {:ok, fetch} = Response.parse_response(v5_response)
      assert fetch.log_start_offset == 50
    end
  end
end
