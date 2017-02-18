defmodule KafkaEx.Stream do
  @moduledoc false
  alias KafkaEx.Protocol.OffsetCommit.Request, as: OffsetCommitRequest
  alias KafkaEx.Protocol.Fetch.Request, as: FetchRequest

  defstruct worker_name: nil, fetch_request: %FetchRequest{}, consumer_group: nil

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
        response = data.worker_name
        |> GenServer.call({:fetch, %{data.fetch_request| offset: offset}})
        |> hd |> Map.get(:partitions) |> hd
        last_offset = response |> Map.get(:last_offset)
        {response.message_set, last_offset}
      end
      Stream.resource(fn -> data.fetch_request.offset end, next_fun, &(&1)).(acc, fun)
    end
    def count(_stream) do
      {:error, __MODULE__}
    end

    def member?(_stream, _item) do
      {:error, __MODULE__}
    end
  end

end
