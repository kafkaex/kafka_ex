# Configure ExUnit to exclude integration and chaos tests by default
ExUnit.configure(
  timeout: 120 * 1000,
  exclude: [
    auth: true,
    consume: true,
    consumer_group: true,
    chaos: true,
    kafka4: true,
    lifecycle: true,
    produce: true,
    rack_awareness: true,
    pin_set: :target_state
  ]
)

# No type_check for NetworkClient: connect/4 takes bare socket options (:binary) its Keyword.t() typespec rejects.
Mimic.copy(KafkaEx.Network.NetworkClient)
Mimic.copy(KafkaEx.Client, type_check: true)

ExUnit.start()

# Check if we need Testcontainers (chaos or kafka4 tests requested)
# Detects --only/--include chaos|kafka4, or ENABLE_TESTCONTAINERS env
testcontainers_tags = ["chaos", "kafka4"]

needs_testcontainers? =
  System.get_env("ENABLE_TESTCONTAINERS") == "true" or
    System.argv()
    |> Enum.chunk_every(2, 1)
    |> Enum.any?(fn
      [flag, tag | _] when flag in ["--only", "--include"] -> hd(String.split(tag, ":")) in testcontainers_tags
      ["--only=" <> tag | _] -> hd(String.split(tag, ":")) in testcontainers_tags
      ["--include=" <> tag | _] -> hd(String.split(tag, ":")) in testcontainers_tags
      _ -> false
    end)

# Start testcontainers GenServer AFTER ExUnit.start() (if available and needed)
# Uses start/1 (not start_link/1) so GenServer survives the caller process exit
if needs_testcontainers? and Code.ensure_loaded?(Testcontainers) do
  # Ensure required applications are started
  {:ok, _} = Application.ensure_all_started(:hackney)

  # Start the Testcontainers GenServer (unlinked, so it survives)
  case Testcontainers.start() do
    {:ok, _pid} ->
      :ok

    {:error, {:already_started, _pid}} ->
      :ok

    {:error, reason} ->
      IO.puts(:stderr, "Warning: Failed to start Testcontainers: #{inspect(reason)}")
      IO.puts(:stderr, "Chaos tests will fail.")
  end
end
