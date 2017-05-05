defmodule KafkaEx.Stream do
  @moduledoc false
  alias KafkaEx.Protocol.OffsetCommit.Request, as: OffsetCommitRequest
  alias KafkaEx.Protocol.Fetch.Request, as: FetchRequest
  alias KafkaEx.Protocol.Fetch.Response, as: FetchResponse

  defstruct worker_name: nil,
    fetch_request: %FetchRequest{},
    consumer_group: nil,
    stream_mode: :infinite

  defimpl Enumerable do
    def reduce(data, acc, fun) do
      next_fun = fn offset ->
        if data.fetch_request.auto_commit do
          GenServer.call(data.worker_name, {
            :offset_commit,
            %OffsetCommitRequest{
              consumer_group: data.consumer_group,
              topic: data.fetch_request.topic,
              partition: data.fetch_request.partition,
              offset: offset, metadata: ""
            }
          })
        end
        response = fetch_response(data, offset)
        if response.error_code == :no_error &&
           response.last_offset != nil && response.last_offset != offset do
          {response.message_set, response.last_offset}
        else
          {mode(data.stream_mode), offset}
        end
      end
      Stream.resource(
        fn -> data.fetch_request.offset end, next_fun, &(&1)
      ).(acc, fun)
    end

    defp mode(:finite), do: :halt
    defp mode(:infinite), do: []

    defp fetch_response(data, offset) do
      req = data.fetch_request
      data.worker_name
      |> GenServer.call({:fetch, %{req| offset: offset}})
      |> FetchResponse.partition_messages(req.topic, req.partition)
    end

    def count(_stream) do
      {:error, __MODULE__}
    end

    def member?(_stream, _item) do
      {:error, __MODULE__}
    end
  end

end
