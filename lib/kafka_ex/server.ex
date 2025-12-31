defmodule KafkaEx.Server do
  @moduledoc """
  Provides helper functions for calling KafkaEx client GenServers.

  This module contains the `call/2,3` function used throughout KafkaEx
  to dispatch requests to client GenServers with proper timeout handling.

  Note: The actual client implementation is `KafkaEx.New.Client`.
  """

  # Default from GenServer
  @default_call_timeout 5_000

  # the timeout parameter is used also for network requests, which
  # means that if we set the same value for the GenServer timeout,
  # it will always timeout first => we won't get the expected behaviour,
  # which is to have the GenServer.call answer with the timeout reply.
  # Instead, we get a GenServer timeout error.
  # To avoid this, we add here an "buffer" time that covers the time
  # needed to process the logic until the network request, and back from it.
  @overhead_timeout 2_000

  @doc """
  Calls a KafkaEx client GenServer with proper timeout handling.

  ## Parameters
    * `server` - The GenServer name or pid
    * `request` - The request tuple/atom to send
    * `opts` - Either a keyword list with `:timeout` key, or an integer timeout

  ## Examples

      KafkaEx.Server.call(:kafka_ex, {:metadata, "my_topic"})
      KafkaEx.Server.call(:kafka_ex, {:produce, request}, timeout: 10_000)
  """
  @spec call(
          GenServer.server(),
          atom | tuple,
          nil | number | (opts :: Keyword.t())
        ) :: term
  def call(server, request, opts \\ [])

  def call(server, request, opts) when is_list(opts) do
    call(server, request, opts[:timeout])
  end

  def call(server, request, nil) do
    # If using the configured sync_timeout that is less than the default
    # GenServer.call timeout, use the larger value unless explicitly set
    # using opts[:timeout].
    timeout =
      max(
        @default_call_timeout,
        Application.get_env(:kafka_ex, :sync_timeout, @default_call_timeout)
      )

    call(server, request, timeout)
  end

  def call(server, request, timeout) when is_integer(timeout) do
    GenServer.call(server, request, timeout + @overhead_timeout)
  end
end
