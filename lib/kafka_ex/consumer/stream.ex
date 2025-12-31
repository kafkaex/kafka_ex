defmodule KafkaEx.Consumer.Stream do
  @moduledoc false

  alias KafkaEx.API, as: KafkaExAPI

  defstruct client: nil,
            topic: nil,
            partition: nil,
            offset: 0,
            consumer_group: nil,
            no_wait_at_logend: false,
            fetch_options: [],
            api_versions: %{fetch: 0, offset_fetch: 0, offset_commit: 0}

  @type t :: %__MODULE__{}

  defimpl Enumerable do
    def reduce(%KafkaEx.Consumer.Stream{} = data, acc, fun) do
      # this function returns a Stream.resource stream, so we need to define
      # start_fun, next_fun, and after_fun callbacks

      # the state payload for the stream is just the offset
      start_fun = fn -> data.offset end

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
           %KafkaEx.Consumer.Stream{
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
           %KafkaEx.Consumer.Stream{no_wait_at_logend: true},
           offset
         ) do
      {:halt, offset}
    end

    # for all other cases we block until messages are ready
    defp stream_control(%{}, %KafkaEx.Consumer.Stream{}, offset) do
      {[], offset}
    end

    ######################################################################

    ######################################################################
    # Offset management

    # first, determine if we even need to commit an offset
    defp maybe_commit_offset(
           fetch_response,
           %KafkaEx.Consumer.Stream{} = stream_data,
           acc
         ) do
      auto_commit = Keyword.get(stream_data.fetch_options, :auto_commit, false)

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
      case Enum.at(message_set, n - 1) do
        nil -> nil
        message -> message.offset
      end
    end

    # otherwise, commit the offset of the last message
    defp last_offset({:cont, _}, message_set) do
      case List.last(message_set) do
        nil -> nil
        message -> message.offset
      end
    end

    defp commit_offset(%KafkaEx.Consumer.Stream{} = stream_data, offset) do
      partitions = [%{partition_num: stream_data.partition, offset: offset}]

      opts = [
        api_version: Map.fetch!(stream_data.api_versions, :offset_commit)
      ]

      KafkaExAPI.commit_offset(
        stream_data.client,
        stream_data.consumer_group,
        stream_data.topic,
        partitions,
        opts
      )
    end

    ######################################################################

    # make the actual fetch request
    defp fetch_response(data, offset) do
      opts =
        data.fetch_options
        |> Keyword.put(:api_version, Map.fetch!(data.api_versions, :fetch))

      case KafkaExAPI.fetch(data.client, data.topic, data.partition, offset, opts) do
        {:ok, fetch_result} ->
          %{
            error_code: :no_error,
            last_offset: fetch_result.last_offset,
            message_set: fetch_result.records
          }

        {:error, error} ->
          %{error_code: error}
      end
    end
  end
end
