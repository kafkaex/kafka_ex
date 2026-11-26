defmodule KafkaEx.Client.NotControllerRetryTest do
  use KafkaEx.TestSupport.PinCase, async: false

  @moduletag pin_set: :invariant

  @not_controller 41
  @topics ["a", "b"]

  setup do
    controller = :atomics.new(1, [])
    :atomics.put(controller, 1, 1)

    pinned =
      start_pinned(
        api_versions: %{metadata: 1..1, create_topics: 0..0, delete_topics: 0..0},
        before_boot: &FakeTransport.respond(&1, :metadata, fn request -> metadata(request, controller) end)
      )

    Map.put(pinned, :controller, controller)
  end

  for {api, field, call} <- [
        {:create_topics, :topics, quote(do: API.create_topics(var!(client), Enum.map(@topics, &%{topic: &1}), 1_000))},
        {:delete_topics, :responses, quote(do: API.delete_topics(var!(client), @topics, 1_000))}
      ] do
    describe "#{api}" do
      test "a broker that is no longer the controller sends the request to the new one", %{
        fake: fake,
        client: client,
        controller: controller
      } do
        FakeTransport.respond(fake, unquote(api), fn request ->
          if request.node_id == 1, do: :atomics.put(controller, 1, 2)
          code = if request.node_id == 1, do: @not_controller, else: 0
          %{unquote(field) => Enum.map(@topics, &%{name: &1, error_code: code})}
        end)

        {result, _log} = with_log(fn -> unquote(call) end)

        assert {:ok, %{topic_results: results}} = result
        assert Enum.all?(results, &(&1.error == :no_error))
        assert nodes(fake, unquote(api)) == [1, 2]
      end

      test "a controller that never answers ends on :not_controller after every attempt", %{
        fake: fake,
        client: client
      } do
        FakeTransport.respond(fake, unquote(api), %{
          unquote(field) => Enum.map(@topics, &%{name: &1, error_code: @not_controller})
        })

        {result, _log} = with_log(fn -> unquote(call) end)

        assert result == {:error, :not_controller}
        assert count(fake, unquote(api)) == Client.retry_count()
      end

      test "a :not_controller for some topics only is the answer, written once", %{fake: fake, client: client} do
        FakeTransport.respond(fake, unquote(api), %{
          unquote(field) => [%{name: "a", error_code: 0}, %{name: "b", error_code: @not_controller}]
        })

        {result, _log} = with_log(fn -> unquote(call) end)

        assert {:ok, %{topic_results: [%{topic: "a", error: :no_error}, %{topic: "b", error: :not_controller}]}} =
                 result

        assert count(fake, unquote(api)) == 1
      end
    end
  end

  defp metadata(request, controller) do
    request.host
    |> metadata_reply(%{@topic => [1, 2]})
    |> Map.put(:controller_id, :atomics.get(controller, 1))
  end
end
