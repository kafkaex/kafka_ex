defmodule KafkaEx.Stream do
  @moduledoc false

  alias KafkaEx.Protocol.OffsetCommit.Request, as: OffsetCommitRequest
  alias KafkaEx.Protocol.Fetch.Request, as: FetchRequest
  alias KafkaEx.Protocol.Fetch.Response, as: FetchResponse

  defstruct worker_name: nil,
    fetch_request: %FetchRequest{},
    consumer_group: nil,
    no_wait_at_logend: false

  defimpl Enumerable do
    def reduce(data = %KafkaEx.Stream{}, acc, fun) do
      next_fun = fn offset ->
        if data.fetch_request.auto_commit do
          commit_offset(data, offset)
        end
        response = fetch_response(data, offset)
        if response.error_code == :no_error && response.last_offset do
           {response.message_set, response.last_offset + 1}
        else
          {stream_control(data.no_wait_at_logend), offset}
        end
      end
      Stream.resource(
        fn ->
          data.fetch_request.offset
        end, next_fun, &(&1)
      ).(acc, fun)
    end

    defp stream_control(true), do: :halt
    defp stream_control(false), do: []

    defp fetch_response(data, offset) do
      req = data.fetch_request
      data.worker_name
      |> GenServer.call({:fetch, %{req| offset: offset}})
      |> FetchResponse.partition_messages(req.topic, req.partition)
    end

    defp commit_offset(stream_data = %KafkaEx.Stream{}, offset) do
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
