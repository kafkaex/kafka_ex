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

  describe "V8 Response implementation" do
    test "parses response (same fields as V5-V7)" do
      record_batch = build_record_batch([%{offset: 80, value: "v8-test"}])

      response =
        build_response("v8-topic", 0, 0, 900, record_batch, %{
          throttle_time_ms: 25,
          partition_header: %{
            last_stable_offset: 850,
            log_start_offset: 400,
            aborted_transactions: nil
          }
        })
        |> Map.put(:__struct__, Kayrock.Fetch.V8.Response)

      assert {:ok, fetch} = Response.parse_response(response)

      assert fetch.topic == "v8-topic"
      assert fetch.partition == 0
      assert fetch.high_watermark == 900
      assert fetch.throttle_time_ms == 25
      assert fetch.last_stable_offset == 850
      assert fetch.log_start_offset == 400
    end

    test "parses response with error code" do
      response =
        build_response("v8-topic", 0, 1, 0, nil)
        |> Map.put(:__struct__, Kayrock.Fetch.V8.Response)

      assert {:error, error} = Response.parse_response(response)
      assert error.error == :offset_out_of_range
    end

    test "returns error for empty responses" do
      response =
        %{responses: []}
        |> Map.put(:__struct__, Kayrock.Fetch.V8.Response)

      assert {:error, error} = Response.parse_response(response)
      assert error.error == :empty_response
    end
  end

  describe "V9 Response implementation" do
    test "parses response (same fields as V5-V8)" do
      record_batch = build_record_batch([%{offset: 90, value: "v9-test"}])

      response =
        build_response("v9-topic", 1, 0, 1000, record_batch, %{
          throttle_time_ms: 30,
          partition_header: %{
            last_stable_offset: 950,
            log_start_offset: 500,
            aborted_transactions: []
          }
        })
        |> Map.put(:__struct__, Kayrock.Fetch.V9.Response)

      assert {:ok, fetch} = Response.parse_response(response)

      assert fetch.topic == "v9-topic"
      assert fetch.partition == 1
      assert fetch.high_watermark == 1000
      assert fetch.throttle_time_ms == 30
      assert fetch.last_stable_offset == 950
      assert fetch.log_start_offset == 500
    end

    test "parses response with error code" do
      response =
        build_response("v9-topic", 0, 6, 0, nil)
        |> Map.put(:__struct__, Kayrock.Fetch.V9.Response)

      assert {:error, error} = Response.parse_response(response)
      assert error.error == :not_leader_for_partition
    end
  end

  describe "V10 Response implementation" do
    test "parses response (same fields as V5-V9)" do
      record_batch = build_record_batch([%{offset: 100, value: "v10-test"}])

      response =
        build_response("v10-topic", 2, 0, 1100, record_batch, %{
          throttle_time_ms: 35,
          partition_header: %{
            last_stable_offset: 1050,
            log_start_offset: 600,
            aborted_transactions: nil
          }
        })
        |> Map.put(:__struct__, Kayrock.Fetch.V10.Response)

      assert {:ok, fetch} = Response.parse_response(response)

      assert fetch.topic == "v10-topic"
      assert fetch.partition == 2
      assert fetch.high_watermark == 1100
      assert fetch.throttle_time_ms == 35
      assert fetch.last_stable_offset == 1050
      assert fetch.log_start_offset == 600
    end

    test "parses response with error code" do
      response =
        build_response("v10-topic", 0, 5, 0, nil)
        |> Map.put(:__struct__, Kayrock.Fetch.V10.Response)

      assert {:error, error} = Response.parse_response(response)
      assert error.error == :leader_not_available
    end
  end

  describe "V11 Response implementation" do
    test "parses response with preferred_read_replica" do
      record_batch = build_record_batch([%{offset: 110, value: "v11-test"}])

      response =
        build_response("v11-topic", 0, 0, 1200, record_batch, %{
          throttle_time_ms: 40,
          partition_header: %{
            last_stable_offset: 1150,
            log_start_offset: 700,
            aborted_transactions: [],
            preferred_read_replica: 3
          }
        })
        |> Map.put(:__struct__, Kayrock.Fetch.V11.Response)

      assert {:ok, fetch} = Response.parse_response(response)

      assert fetch.topic == "v11-topic"
      assert fetch.partition == 0
      assert fetch.high_watermark == 1200
      assert fetch.throttle_time_ms == 40
      assert fetch.last_stable_offset == 1150
      assert fetch.log_start_offset == 700
      assert fetch.preferred_read_replica == 3
    end

    test "parses response with preferred_read_replica = -1 (no preference)" do
      record_batch = build_record_batch([%{offset: 0, value: "test"}])

      response =
        build_response("test-topic", 0, 0, 100, record_batch, %{
          throttle_time_ms: 0,
          partition_header: %{
            last_stable_offset: 90,
            log_start_offset: 0,
            aborted_transactions: nil,
            preferred_read_replica: -1
          }
        })
        |> Map.put(:__struct__, Kayrock.Fetch.V11.Response)

      assert {:ok, fetch} = Response.parse_response(response)

      assert fetch.preferred_read_replica == -1
    end

    test "parses response with full RecordBatch data including headers" do
      record_batch =
        build_record_batch([
          %{offset: 200, key: "k1", value: "v1", timestamp: 5000},
          %{
            offset: 201,
            key: "k2",
            value: "v2",
            timestamp: 5001,
            headers: [{"trace-id", "abc"}, {"content-type", "json"}]
          }
        ])

      response =
        build_response("full-topic", 1, 0, 2000, record_batch, %{
          throttle_time_ms: 5,
          partition_header: %{
            last_stable_offset: 1999,
            log_start_offset: 100,
            aborted_transactions: [],
            preferred_read_replica: 2
          }
        })
        |> Map.put(:__struct__, Kayrock.Fetch.V11.Response)

      assert {:ok, fetch} = Response.parse_response(response)

      assert length(fetch.records) == 2
      assert fetch.last_offset == 201
      assert fetch.preferred_read_replica == 2

      [r1, r2] = fetch.records
      assert r1.key == "k1"
      assert r1.value == "v1"

      assert r2.headers == [
               %Header{key: "trace-id", value: "abc"},
               %Header{key: "content-type", value: "json"}
             ]
    end

    test "parses response with error code" do
      response =
        build_response("v11-topic", 0, 3, 0, nil)
        |> Map.put(:__struct__, Kayrock.Fetch.V11.Response)

      assert {:error, error} = Response.parse_response(response)
      assert error.error == :unknown_topic_or_partition
    end

    test "returns error for empty responses" do
      response =
        %{responses: []}
        |> Map.put(:__struct__, Kayrock.Fetch.V11.Response)

      assert {:error, error} = Response.parse_response(response)
      assert error.error == :empty_response
    end
  end

  describe "Any fallback response implementation (forward compatibility)" do
    defmodule FakeV12Response do
      defstruct [:throttle_time_ms, :responses]
    end

    defmodule FakeMinimalResponse do
      defstruct [:responses]
    end

    test "Any fallback extracts all available fields" do
      record_batch = build_record_batch([%{offset: 50, value: "future-data"}])

      response =
        build_response("future-topic", 0, 0, 500, record_batch, %{
          throttle_time_ms: 42,
          partition_header: %{
            last_stable_offset: 450,
            log_start_offset: 100,
            aborted_transactions: [],
            preferred_read_replica: 5
          }
        })
        |> Map.put(:__struct__, FakeV12Response)

      assert {:ok, %Fetch{} = fetch} = Response.parse_response(response)
      assert fetch.topic == "future-topic"
      assert fetch.throttle_time_ms == 42
      assert fetch.last_stable_offset == 450
      assert fetch.log_start_offset == 100
      assert fetch.preferred_read_replica == 5
    end

    test "Any fallback handles response without throttle_time_ms" do
      message_set = build_message_set([%{offset: 0, value: "minimal-data"}])

      response =
        build_response("minimal-topic", 0, 0, 100, message_set)
        |> Map.put(:__struct__, FakeMinimalResponse)

      assert {:ok, %Fetch{} = fetch} = Response.parse_response(response)
      assert fetch.topic == "minimal-topic"
      assert fetch.throttle_time_ms == nil
    end

    test "Any fallback handles error code from unknown struct" do
      response =
        build_response("test", 0, 7, 0, nil)
        |> Map.put(:__struct__, FakeV12Response)

      assert {:error, error} = Response.parse_response(response)
      assert error.error == :request_timed_out
    end
  end

  describe "parse_response via KayrockProtocol" do
    alias KafkaEx.Protocol.KayrockProtocol

    test "dispatches V0 response through parse_response" do
      message_set = build_message_set([%{offset: 0, value: "test"}])

      response =
        build_response("test-topic", 0, 0, 100, message_set)
        |> Map.put(:__struct__, Kayrock.Fetch.V0.Response)

      assert {:ok, %Fetch{} = fetch} = KayrockProtocol.parse_response(:fetch, response)
      assert fetch.topic == "test-topic"
    end

    test "dispatches V5 response through parse_response" do
      record_batch = build_record_batch([%{offset: 10, value: "test"}])

      response =
        build_response("test-topic", 0, 0, 200, record_batch, %{
          throttle_time_ms: 5,
          partition_header: %{
            last_stable_offset: 190,
            log_start_offset: 50,
            aborted_transactions: nil
          }
        })
        |> Map.put(:__struct__, Kayrock.Fetch.V5.Response)

      assert {:ok, %Fetch{} = fetch} = KayrockProtocol.parse_response(:fetch, response)
      assert fetch.log_start_offset == 50
      assert fetch.throttle_time_ms == 5
    end

    test "dispatches V11 response through parse_response" do
      record_batch = build_record_batch([%{offset: 20, value: "test"}])

      response =
        build_response("test-topic", 0, 0, 300, record_batch, %{
          throttle_time_ms: 10,
          partition_header: %{
            last_stable_offset: 290,
            log_start_offset: 100,
            aborted_transactions: [],
            preferred_read_replica: 2
          }
        })
        |> Map.put(:__struct__, Kayrock.Fetch.V11.Response)

      assert {:ok, %Fetch{} = fetch} = KayrockProtocol.parse_response(:fetch, response)
      assert fetch.preferred_read_replica == 2
      assert fetch.throttle_time_ms == 10
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

    test "only V11 exposes preferred_read_replica" do
      record_batch = build_record_batch([%{offset: 0, value: "test"}])

      # V10 has the field in Kayrock but our impl doesn't extract it
      v10_response =
        build_response("test-topic", 0, 0, 100, record_batch, %{
          throttle_time_ms: 0,
          partition_header: %{
            last_stable_offset: 90,
            log_start_offset: 0,
            aborted_transactions: nil
          }
        })
        |> Map.put(:__struct__, Kayrock.Fetch.V10.Response)

      assert {:ok, fetch10} = Response.parse_response(v10_response)
      assert fetch10.preferred_read_replica == nil

      # V11 extracts it
      v11_response =
        build_response("test-topic", 0, 0, 100, record_batch, %{
          throttle_time_ms: 0,
          partition_header: %{
            last_stable_offset: 90,
            log_start_offset: 0,
            aborted_transactions: nil,
            preferred_read_replica: 2
          }
        })
        |> Map.put(:__struct__, Kayrock.Fetch.V11.Response)

      assert {:ok, fetch11} = Response.parse_response(v11_response)
      assert fetch11.preferred_read_replica == 2
    end

    test "V0-V11 all produce consistent base fields for equivalent data" do
      # Build equivalent responses for each major version boundary
      record_batch = build_record_batch([%{offset: 42, value: "data"}])

      v0_resp =
        build_response("test", 0, 0, 100, build_message_set([%{offset: 42, value: "data"}]))
        |> Map.put(:__struct__, Kayrock.Fetch.V0.Response)

      v1_resp =
        build_response("test", 0, 0, 100, build_message_set([%{offset: 42, value: "data"}]), %{
          throttle_time_ms: 0
        })
        |> Map.put(:__struct__, Kayrock.Fetch.V1.Response)

      v5_resp =
        build_response("test", 0, 0, 100, record_batch, %{
          throttle_time_ms: 0,
          partition_header: %{last_stable_offset: 90, log_start_offset: 0, aborted_transactions: nil}
        })
        |> Map.put(:__struct__, Kayrock.Fetch.V5.Response)

      v8_resp =
        build_response("test", 0, 0, 100, record_batch, %{
          throttle_time_ms: 0,
          partition_header: %{last_stable_offset: 90, log_start_offset: 0, aborted_transactions: nil}
        })
        |> Map.put(:__struct__, Kayrock.Fetch.V8.Response)

      v11_resp =
        build_response("test", 0, 0, 100, record_batch, %{
          throttle_time_ms: 0,
          partition_header: %{
            last_stable_offset: 90,
            log_start_offset: 0,
            aborted_transactions: nil,
            preferred_read_replica: -1
          }
        })
        |> Map.put(:__struct__, Kayrock.Fetch.V11.Response)

      {:ok, r0} = Response.parse_response(v0_resp)
      {:ok, r1} = Response.parse_response(v1_resp)
      {:ok, r5} = Response.parse_response(v5_resp)
      {:ok, r8} = Response.parse_response(v8_resp)
      {:ok, r11} = Response.parse_response(v11_resp)

      # All versions produce the same core fields
      for r <- [r0, r1, r5, r8, r11] do
        assert %Fetch{} = r
        assert r.topic == "test"
        assert r.partition == 0
        assert r.high_watermark == 100
        assert r.last_offset == 42
        assert length(r.records) == 1
      end

      # V0 has no throttle_time_ms
      assert r0.throttle_time_ms == nil
      # V1+ have throttle_time_ms
      assert r1.throttle_time_ms == 0
      assert r5.throttle_time_ms == 0

      # V0-V3 have no last_stable_offset
      assert r0.last_stable_offset == nil
      # V5+ have last_stable_offset
      assert r5.last_stable_offset == 90
      assert r8.last_stable_offset == 90

      # V0-V4 have no log_start_offset
      assert r0.log_start_offset == nil
      # V5+ have log_start_offset
      assert r5.log_start_offset == 0
      assert r8.log_start_offset == 0

      # V0-V10 have no preferred_read_replica
      assert r0.preferred_read_replica == nil
      assert r5.preferred_read_replica == nil
      assert r8.preferred_read_replica == nil
      # V11 has preferred_read_replica
      assert r11.preferred_read_replica == -1
    end
  end
end
