defmodule KafkaEx.ConsumerGroup.Test do
  alias KafkaEx.Protocol, as: Proto
  alias KafkaEx.Config
  use ExUnit.Case
  import TestHelper

  @moduletag :consumer_group

  test "fetching the consumer group from the default worker" do
    assert Application.get_env(:kafka_ex, :consumer_group) ==
             KafkaEx.consumer_group()
  end

  test "create_worker returns an error when an invalid consumer group is provided" do
    assert {:error, :invalid_consumer_group} ==
             KafkaEx.create_worker(:francine, consumer_group: 0)
  end

  test "create_worker allows us to disable the consumer group" do
    {:ok, pid} = KafkaEx.create_worker(:barney, consumer_group: :no_consumer_group)

    consumer_group = :sys.get_state(pid).consumer_group
    assert consumer_group == :no_consumer_group
  end

  describe "custom ssl options" do
    setup do
      # reset application env after each test
      env_before = Application.get_all_env(:kafka_ex)

      ssl_option_filenames = ["ca-cert", "cert.pem", "key.pem"]
      {:ok, cwd} = File.cwd()

      original_filenames =
        ssl_option_filenames
        |> Enum.map(fn filename -> Path.join([cwd, "ssl", filename]) end)

      target_filenames =
        ssl_option_filenames
        |> Enum.map(fn filename ->
          Path.rootname(filename) <> "-custom" <> Path.extname(filename)
        end)
        |> Enum.map(fn filename -> Path.join([cwd, "ssl", filename]) end)

      List.zip([original_filenames, target_filenames])
      |> Enum.map(fn {original, target} -> File.copy(original, target) end)

      on_exit(fn ->
        # this is basically Application.put_all_env
        for {k, v} <- env_before do
          Application.put_env(:kafka_ex, k, v)
        end

        target_filenames
        |> Enum.map(fn filename -> File.rm(filename) end)

        :ok
      end)

      :ok
    end

    test "create_worker allows us to pass in use_ssl and ssl_options options" do
      Application.put_env(:kafka_ex, :use_ssl, true)
      ssl_options = Application.get_env(:kafka_ex, :ssl_options)
      assert ssl_options == Config.ssl_options()

      ## These reference symbolic links to the original files in order to validate
      ## that custom SSL filepaths can specified
      custom_ssl_options = [
        cacertfile: File.cwd!() <> "/ssl/ca-cert-custom",
        certfile: File.cwd!() <> "/ssl/cert-custom.pem",
        keyfile: File.cwd!() <> "/ssl/key-custom.pem"
      ]

      {:ok, pid} =
        KafkaEx.create_worker(:real,
          use_ssl: true,
          ssl_options: custom_ssl_options
        )

      consumer_group = :sys.get_state(pid)
      assert consumer_group.ssl_options == custom_ssl_options
      refute consumer_group.ssl_options == ssl_options
      assert consumer_group.use_ssl == true
    end
  end

  test "create_worker allows us to provide a consumer group" do
    {:ok, pid} = KafkaEx.create_worker(:bah, consumer_group: "my_consumer_group")

    consumer_group = :sys.get_state(pid).consumer_group

    assert consumer_group == "my_consumer_group"
  end

  test "create_worker allows custom consumer_group_update_interval" do
    {:ok, pid} =
      KafkaEx.create_worker(
        :consumer_group_update_interval_custom,
        uris: uris(),
        consumer_group_update_interval: 10
      )

    consumer_group_update_interval = :sys.get_state(pid).consumer_group_update_interval

    assert consumer_group_update_interval == 10
  end

  test "create_worker provides a default consumer_group_update_interval of '30000'" do
    {:ok, pid} = KafkaEx.create_worker(:de, uris: uris())

    consumer_group_update_interval = :sys.get_state(pid).consumer_group_update_interval

    assert consumer_group_update_interval == 30000
  end

  test "create_worker provides a default consumer_group of 'kafka_ex'" do
    {:ok, pid} = KafkaEx.create_worker(:baz, uris: uris())
    consumer_group = :sys.get_state(pid).consumer_group

    assert consumer_group == "kafka_ex"
  end

  test "create_worker takes a consumer_group option and sets that as the consumer_group of the worker" do
    {:ok, pid} = KafkaEx.create_worker(:joe, uris: uris(), consumer_group: "foo")

    consumer_group = :sys.get_state(pid).consumer_group

    assert consumer_group == "foo"
  end

  test "asking the worker for the name of its consumer group" do
    consumer_group = "this_is_my_consumer_group"
    worker_name = :consumer_group_reader_test

    {:ok, _pid} =
      KafkaEx.create_worker(
        worker_name,
        consumer_group: consumer_group
      )

    assert consumer_group == KafkaEx.consumer_group(worker_name)
  end

  test "consumer_group_metadata works" do
    random_string = generate_random_string()

    KafkaEx.produce(%Proto.Produce.Request{
      topic: "food",
      partition: 0,
      required_acks: 1,
      messages: [%Proto.Produce.Message{value: "hey"}]
    })

    KafkaEx.fetch("food", 0, offset: 0)

    KafkaEx.create_worker(
      :consumer_group_metadata_worker,
      consumer_group: random_string,
      uris: Application.get_env(:kafka_ex, :brokers)
    )

    pid = Process.whereis(:consumer_group_metadata_worker)

    metadata =
      KafkaEx.consumer_group_metadata(
        :consumer_group_metadata_worker,
        random_string
      )

    consumer_group_metadata = :sys.get_state(pid).consumer_metadata

    assert metadata != %Proto.ConsumerMetadata.Response{}
    assert metadata.coordinator_host != nil
    assert metadata.error_code == :no_error
    assert metadata == consumer_group_metadata
  end

  # update_consumer_metadata
  test "worker updates metadata after specified interval" do
    {:ok, pid} =
      KafkaEx.create_worker(
        :update_consumer_metadata,
        uris: uris(),
        consumer_group: "kafka_ex",
        consumer_group_update_interval: 100
      )

    consumer_metadata = %KafkaEx.Protocol.ConsumerMetadata.Response{}

    :sys.replace_state(pid, fn state ->
      %{state | :consumer_metadata => consumer_metadata}
    end)

    :timer.sleep(105)
    new_consumer_metadata = :sys.get_state(pid).consumer_metadata

    refute new_consumer_metadata == consumer_metadata
  end

  test "worker does not update metadata when consumer_group is disabled" do
    {:ok, pid} =
      KafkaEx.create_worker(
        :no_consumer_metadata_update,
        uris: uris(),
        consumer_group: :no_consumer_group,
        consumer_group_update_interval: 100
      )

    consumer_metadata = %KafkaEx.Protocol.ConsumerMetadata.Response{}

    :sys.replace_state(pid, fn state ->
      %{state | :consumer_metadata => consumer_metadata}
    end)

    :timer.sleep(105)
    new_consumer_metadata = :sys.get_state(pid).consumer_metadata

    assert new_consumer_metadata == consumer_metadata
  end

  # fetch
  test "fetch auto_commits offset by default" do
    worker_name = :fetch_test_worker
    topic = "kafka_ex_consumer_group_test"
    consumer_group = "auto_commit_consumer_group"

    KafkaEx.create_worker(
      worker_name,
      uris: Application.get_env(:kafka_ex, :brokers),
      consumer_group: consumer_group
    )

    offset_before = TestHelper.latest_offset_number(topic, 0, worker_name)

    Enum.each(1..10, fn _ ->
      msg = %Proto.Produce.Message{value: "hey #{inspect(:os.timestamp())}"}

      KafkaEx.produce(
        %Proto.Produce.Request{
          topic: topic,
          partition: 0,
          required_acks: 1,
          messages: [msg]
        },
        worker_name: worker_name
      )
    end)

    offset_after = TestHelper.latest_offset_number(topic, 0, worker_name)
    assert offset_after == offset_before + 10

    [logs] =
      KafkaEx.fetch(
        topic,
        0,
        offset: offset_before,
        worker_name: worker_name
      )

    [partition] = logs.partitions
    message_set = partition.message_set
    assert 10 == length(message_set)

    last_message = List.last(message_set)
    offset_of_last_message = last_message.offset

    offset_request = %Proto.OffsetFetch.Request{
      topic: topic,
      partition: 0,
      consumer_group: consumer_group
    }

    [offset_fetch_response] = KafkaEx.offset_fetch(worker_name, offset_request)
    [partition] = offset_fetch_response.partitions
    error_code = partition.error_code
    offset_fetch_response_offset = partition.offset

    assert error_code == :no_error
    assert offset_of_last_message == offset_fetch_response_offset
  end

  test "fetch starts consuming from last committed offset" do
    random_string = generate_random_string()
    KafkaEx.create_worker(:fetch_test_committed_worker)

    Enum.each(1..10, fn _ ->
      KafkaEx.produce(%Proto.Produce.Request{
        topic: random_string,
        partition: 0,
        required_acks: 1,
        messages: [%Proto.Produce.Message{value: "hey"}]
      })
    end)

    KafkaEx.offset_commit(
      :fetch_test_committed_worker,
      %Proto.OffsetCommit.Request{topic: random_string, offset: 3, partition: 0}
    )

    logs =
      KafkaEx.fetch(random_string, 0, worker: :fetch_test_committed_worker)
      |> hd
      |> Map.get(:partitions)
      |> hd
      |> Map.get(:message_set)

    first_message = logs |> hd

    assert first_message.offset == 4
    assert length(logs) == 6
  end

  test "fetch does not commit offset with auto_commit is set to false" do
    topic = generate_random_string()
    worker_name = :fetch_no_auto_commit_worker
    KafkaEx.create_worker(worker_name)

    Enum.each(1..10, fn _ ->
      KafkaEx.produce(
        %Proto.Produce.Request{
          topic: topic,
          partition: 0,
          required_acks: 1,
          messages: [%Proto.Produce.Message{value: "hey"}]
        },
        worker_name: worker_name
      )
    end)

    offset =
      KafkaEx.fetch(
        topic,
        0,
        offset: 0,
        worker: worker_name,
        auto_commit: false
      )
      |> hd
      |> Map.get(:partitions)
      |> hd
      |> Map.get(:message_set)
      |> Enum.reverse()
      |> hd
      |> Map.get(:offset)

    offset_fetch_response =
      KafkaEx.offset_fetch(worker_name, %Proto.OffsetFetch.Request{
        topic: topic,
        partition: 0
      })
      |> hd

    offset_fetch_response_offset = offset_fetch_response.partitions |> hd |> Map.get(:offset)

    refute offset == offset_fetch_response_offset
  end

  # offset_fetch
  test "offset_fetch does not override consumer_group" do
    topic = generate_random_string()
    worker_name = :offset_fetch_consumer_group
    consumer_group = "bar#{topic}"

    KafkaEx.create_worker(
      worker_name,
      consumer_group: consumer_group,
      uris: Application.get_env(:kafka_ex, :brokers)
    )

    Enum.each(1..10, fn _ ->
      KafkaEx.produce(
        %Proto.Produce.Request{
          topic: topic,
          required_acks: 1,
          partition: 0,
          messages: [%Proto.Produce.Message{value: "hey"}]
        },
        worker_name: worker_name
      )
    end)

    KafkaEx.offset_fetch(worker_name, %KafkaEx.Protocol.OffsetFetch.Request{
      topic: topic,
      partition: 0
    })

    assert :sys.get_state(:offset_fetch_consumer_group).consumer_group ==
             consumer_group
  end

  # offset_commit
  test "offset_commit commits an offset and offset_fetch retrieves the committed offset" do
    random_string = generate_random_string()

    Enum.each(1..10, fn _ ->
      KafkaEx.produce(%Proto.Produce.Request{
        topic: random_string,
        partition: 0,
        required_acks: 1,
        messages: [%Proto.Produce.Message{value: "hey"}]
      })
    end)

    assert KafkaEx.offset_commit(
             Config.default_worker(),
             %Proto.OffsetCommit.Request{
               topic: random_string,
               offset: 9,
               partition: 0
             }
           ) ==
             [
               %Proto.OffsetCommit.Response{
                 partitions: [0],
                 topic: random_string
               }
             ]

    assert KafkaEx.offset_fetch(
             Config.default_worker(),
             %Proto.OffsetFetch.Request{topic: random_string, partition: 0}
           ) ==
             [
               %Proto.OffsetFetch.Response{
                 partitions: [
                   %{
                     metadata: "",
                     error_code: :no_error,
                     offset: 9,
                     partition: 0
                   }
                 ],
                 topic: random_string
               }
             ]
  end

  # stream
  test "stream auto_commits offset by default" do
    random_string = generate_random_string()

    KafkaEx.create_worker(
      :stream_auto_commit,
      uris: uris(),
      consumer_group: "kafka_ex"
    )

    KafkaEx.produce(%Proto.Produce.Request{
      topic: random_string,
      partition: 0,
      required_acks: 1,
      messages: [
        %Proto.Produce.Message{value: "hey"},
        %Proto.Produce.Message{value: "hi"}
      ]
    })

    stream =
      KafkaEx.stream(
        random_string,
        0,
        worker_name: :stream_auto_commit,
        offset: 0
      )

    log = TestHelper.wait_for_any(fn -> Enum.take(stream, 2) end)

    refute Enum.empty?(log)

    [offset_fetch_response | _] =
      KafkaEx.offset_fetch(:stream_auto_commit, %Proto.OffsetFetch.Request{
        topic: random_string,
        partition: 0
      })

    error_code = offset_fetch_response.partitions |> hd |> Map.get(:error_code)
    offset = offset_fetch_response.partitions |> hd |> Map.get(:offset)

    assert error_code == :no_error
    refute offset == 0
  end

  test "stream starts consuming from the next offset" do
    random_string = generate_random_string()
    consumer_group = "kafka_ex"
    worker_name = :stream_last_committed_offset

    KafkaEx.create_worker(
      worker_name,
      uris: uris(),
      consumer_group: consumer_group
    )

    Enum.each(1..10, fn _ ->
      KafkaEx.produce(
        %Proto.Produce.Request{
          topic: random_string,
          partition: 0,
          required_acks: 1,
          messages: [%Proto.Produce.Message{value: "hey"}]
        },
        worker_name: worker_name
      )
    end)

    KafkaEx.offset_commit(worker_name, %Proto.OffsetCommit.Request{
      topic: random_string,
      partition: 0,
      offset: 3
    })

    # make sure the offset commit is actually committed before we
    # start streaming again
    :ok =
      TestHelper.wait_for(fn ->
        3 ==
          TestHelper.latest_consumer_offset_number(
            random_string,
            0,
            consumer_group,
            worker_name
          )
      end)

    stream = KafkaEx.stream(random_string, 0, worker_name: worker_name)
    log = TestHelper.wait_for_any(fn -> Enum.take(stream, 2) end)

    refute Enum.empty?(log)
    first_message = log |> hd
    assert first_message.offset == 4
  end

  test "stream auto_commit deals with small batches correctly" do
    topic_name = generate_random_string()
    consumer_group = "stream_test"

    KafkaEx.create_worker(:stream, uris: uris())

    KafkaEx.produce(
      %Proto.Produce.Request{
        topic: topic_name,
        partition: 0,
        required_acks: 1,
        messages: [
          %Proto.Produce.Message{value: "message 2"},
          %Proto.Produce.Message{value: "message 3"},
          %Proto.Produce.Message{value: "message 4"},
          %Proto.Produce.Message{value: "message 5"}
        ]
      },
      worker_name: :stream
    )

    stream =
      KafkaEx.stream(
        topic_name,
        0,
        worker_name: :stream,
        offset: 0,
        auto_commit: true,
        consumer_group: consumer_group
      )

    [m1, m2] = Enum.take(stream, 2)
    assert "message 2" == m1.value
    assert "message 3" == m2.value

    offset =
      TestHelper.latest_consumer_offset_number(
        topic_name,
        0,
        consumer_group,
        :stream
      )

    assert offset == m2.offset
  end

  test "stream auto_commit doesn't exceed the end of the log" do
    topic_name = generate_random_string()
    consumer_group = "stream_test"

    KafkaEx.create_worker(:stream, uris: uris())

    KafkaEx.produce(
      %Proto.Produce.Request{
        topic: topic_name,
        partition: 0,
        required_acks: 1,
        messages: [
          %Proto.Produce.Message{value: "message 2"},
          %Proto.Produce.Message{value: "message 3"},
          %Proto.Produce.Message{value: "message 4"},
          %Proto.Produce.Message{value: "message 5"}
        ]
      },
      worker_name: :stream
    )

    stream =
      KafkaEx.stream(
        topic_name,
        0,
        worker_name: :stream,
        offset: 0,
        no_wait_at_logend: true,
        auto_commit: true,
        consumer_group: consumer_group
      )

    [m1, m2, m3, m4] = Enum.take(stream, 10)
    assert "message 2" == m1.value
    assert "message 3" == m2.value
    assert "message 4" == m3.value
    assert "message 5" == m4.value

    offset =
      TestHelper.latest_consumer_offset_number(
        topic_name,
        0,
        consumer_group,
        :stream
      )

    assert offset == m4.offset

    other_consumer_group = "another_consumer_group"

    map_stream =
      KafkaEx.stream(
        topic_name,
        0,
        worker_name: :stream,
        no_wait_at_logend: true,
        auto_commit: true,
        offset: 0,
        consumer_group: other_consumer_group
      )

    assert ["message 2", "message 3", "message 4", "message 5"] =
             Enum.map(map_stream, fn m -> m.value end)

    offset =
      TestHelper.latest_consumer_offset_number(
        topic_name,
        0,
        other_consumer_group,
        :stream
      )

    # should have the same offset as the first stream
    assert offset == m4.offset
  end

  test "streams with a consumer group begin at the last committed offset" do
    topic_name = generate_random_string()
    consumer_group = "stream_test"

    KafkaEx.create_worker(:stream, uris: uris())

    messages_in =
      Enum.map(
        1..10,
        fn ix -> %Proto.Produce.Message{value: "Msg #{ix}"} end
      )

    KafkaEx.produce(
      %Proto.Produce.Request{
        topic: topic_name,
        partition: 0,
        required_acks: 1,
        messages: messages_in
      },
      worker_name: :stream
    )

    stream1 =
      KafkaEx.stream(
        topic_name,
        0,
        worker_name: :stream,
        no_wait_at_logend: true,
        auto_commit: true,
        offset: 0,
        consumer_group: consumer_group
      )

    assert ["Msg 1", "Msg 2"] ==
             stream1
             |> Enum.take(2)
             |> Enum.map(fn msg -> msg.value end)

    stream2 =
      KafkaEx.stream(
        topic_name,
        0,
        worker_name: :stream,
        no_wait_at_logend: true,
        auto_commit: true,
        consumer_group: consumer_group
      )

    message_values =
      stream2
      |> Enum.take(2)
      |> Enum.map(fn msg -> msg.value end)

    assert ["Msg 3", "Msg 4"] == message_values
  end
end
