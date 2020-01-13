defmodule KafkaEx.Stream do
  @moduledoc false

  alias KafkaEx.Server
  alias KafkaEx.Protocol.OffsetCommit.Request, as: OffsetCommitRequest
  alias KafkaEx.Protocol.Fetch.Request, as: FetchRequest
  alias KafkaEx.Protocol.Fetch.Response, as: FetchResponse

  defstruct worker_name: nil,
            fetch_request: %FetchRequest{},
            consumer_group: nil,
            no_wait_at_logend: false,
            api_versions: %{fetch: 0, offset_fetch: 0, offset_commit: 0}

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
        |> fetch_response(offset)
        |> maybe_commit_offset(data, acc)
        |> stream_control(data, offset)
      end

      # there isn't really any cleanup, so we don't need to do anything with
      # the after_fun callback
      after_fun = fn _last_offset -> :ok end

      Stream.resource(start_fun, next_fun, after_fun).(acc, fun)
    end

    def count(_stream) do
      {:error, __MODULE__}
    end

    def member?(_stream, _item) do
      {:error, __MODULE__}
    end

    def slice(_stream) do
      {:error, __MODULE__}
    end

    ######################################################################
    # Main stream flow control

    # if we get an empty response, we block until messages are ready
    defp stream_control(
           %{
             error_code: :no_error,
             last_offset: last_offset,
             message_set: []
           },
           %KafkaEx.Stream{
             no_wait_at_logend: false
           },
           _offset
         )
         when is_integer(last_offset) do
      {[], last_offset}
    end

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
         )
         when is_integer(last_offset) do
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

    ######################################################################
    # Offset management

    # first, determine if we even need to commit an offset
    defp maybe_commit_offset(
           fetch_response,
           %KafkaEx.Stream{
             fetch_request: %FetchRequest{auto_commit: auto_commit}
           } = stream_data,
           acc
         ) do
      if need_commit?(fetch_response, auto_commit) do
        offset_to_commit = last_offset(acc, fetch_response.message_set)
        commit_offset(stream_data, offset_to_commit)
      end

      fetch_response
    end

    # no response -> no commit
    defp need_commit?(fetch_response, _auto_commit)
         when fetch_response == %{},
         do: false

    # no messages in response -> no commit
    defp need_commit?(%{message_set: []}, _auto_commit), do: false
    # otherwise, use the auto_commit setting
    defp need_commit?(_fetch_response, auto_commit), do: auto_commit

    # if we have requested fewer messages than we fetched, commit the offset
    # of the last one we will actually consume
    defp last_offset({:cont, {_, n}}, message_set)
         when n <= length(message_set) do
      message = Enum.at(message_set, n - 1)
      message.offset
    end

    # otherwise, commit the offset of the last message
    defp last_offset({:cont, _}, message_set) do
      message = List.last(message_set)
      message.offset
    end

    defp commit_offset(%KafkaEx.Stream{} = stream_data, offset) do
      Server.call(stream_data.worker_name, {
        :offset_commit,
        %OffsetCommitRequest{
          consumer_group: stream_data.consumer_group,
          topic: stream_data.fetch_request.topic,
          partition: stream_data.fetch_request.partition,
          offset: offset,
          metadata: "",
          api_version: Map.fetch!(stream_data.api_versions, :offset_commit)
        }
      })
    end

    ######################################################################

    # make the actual fetch request
    defp fetch_response(data, offset) do
      req = data.fetch_request

      # note we set auto_commit: false in the actual request because the stream
      # processor handles commits on its own
      data.worker_name
      |> Server.call({:fetch, %{req | offset: offset, auto_commit: false}})
      |> FetchResponse.partition_messages(req.topic, req.partition)
    end
  end
end
