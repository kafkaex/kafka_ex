defmodule KafkaEx.API.Behaviour do
  @moduledoc """
  Behaviour for modules using `KafkaEx.API`.

  This behaviour defines the callback that must be implemented when
  using `KafkaEx.API` in your module.

  ## Example

      defmodule MyApp.Kafka do
        use KafkaEx.API

        @impl KafkaEx.API.Behaviour
        def client do
          MyApp.KafkaClient
        end
      end
  """

  @doc """
  Returns the client pid or name to use for API calls.

  This callback is automatically injected when you `use KafkaEx.API`
  with the `:client` option. If you don't provide the `:client` option,
  you must implement this callback yourself.
  """
  @callback client() :: GenServer.server()
end
