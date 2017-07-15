defmodule KafkaEx.Stream do
  @moduledoc false

  alias KafkaEx.Protocol.OffsetCommit.Request, as: OffsetCommitRequest
  alias KafkaEx.Protocol.Fetch.Request, as: FetchRequest
  alias KafkaEx.Protocol.Fetch.Response, as: FetchResponse

  defstruct worker_name: nil,
    fetch_request: %FetchRequest{},
    consumer_group: nil,
    no_wait_at_logend: false

  @type t :: %__MODULE__{}

  defimpl Enumerable do
    def reduce(%KafkaEx.Stream{} = data, acc, fun) do
      # this function returns a Stream.resource stream, so we need to define
      # start_fun, next_fun, and after_fun callbacks

      # the state payload for the stream is just the offset
      start_fun = fn -> data.fetch_request.offset end

      # each iteration we need to take care of fetching and (possibly)
      # committing offsets
      next_fun = fn offset ->
        data
        |> maybe_commit_offset(offset)
        |> fetch_response(offset)
        |> stream_control(data, offset)
      end

      # there isn't really any cleanup, so we don't need to do anything with
      # the after_fun callback
      after_fun = fn(_last_offset) -> :ok end

      Stream.resource(start_fun, next_fun, after_fun).(acc, fun)
    end

    ######################################################################
    # Main stream flow control

    # if we get a response, we return the message set and point at the next
    # offset after the last message
    defp stream_control(
      %{
        error_code: :no_error,
        last_offset: last_offset,
        message_set: message_set
      },
      _stream_data,
      _offset
    ) when is_integer(last_offset) do
      {message_set, last_offset + 1}
    end

    # if we don't get any messages and no_wait_at_logend is true, we halt
    defp stream_control(
      %{},
      %KafkaEx.Stream{no_wait_at_logend: true},
      offset
    ) do
      {:halt, offset}
    end

    # for all other cases we block until messages are ready
    defp stream_control(%{}, %KafkaEx.Stream{}, offset) do
      {[], offset}
    end
    ######################################################################

    defp fetch_response(data, offset) do
      req = data.fetch_request
      data.worker_name
      |> GenServer.call({:fetch, %{req| offset: offset}})
      |> FetchResponse.partition_messages(req.topic, req.partition)
    end

    defp maybe_commit_offset(
      %KafkaEx.Stream{
        fetch_request: %FetchRequest{auto_commit: true}
      } = stream_data,
      offset
    ) do
      commit_offset(stream_data, offset)
      stream_data
    end

    defp maybe_commit_offset(%KafkaEx.Stream{} = stream_data, _offset) do
      stream_data
    end

    defp commit_offset(%KafkaEx.Stream{} = stream_data, offset) do
      GenServer.call(stream_data.worker_name, {
        :offset_commit,
        %OffsetCommitRequest{
          consumer_group: stream_data.consumer_group,
          topic: stream_data.fetch_request.topic,
          partition: stream_data.fetch_request.partition,
          offset: offset, metadata: ""
        }
      })
    end

    def count(_stream) do
      {:error, __MODULE__}
    end

    def member?(_stream, _item) do
      {:error, __MODULE__}
    end
  end
end
