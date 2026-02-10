# Configure ExUnit to exclude integration and chaos tests by default
ExUnit.configure(
  timeout: 120 * 1000,
  exclude: [
    auth: true,
    consume: true,
    consumer_group: true,
    chaos: true,
    lifecycle: true,
    produce: true
  ]
)

ExUnit.start()

{:ok, _} = Application.ensure_all_started(:hammox)

# Check if we need Testcontainers (chaos tests requested)
# Detects --only chaos, --include chaos, or ENABLE_TESTCONTAINERS env
needs_testcontainers? =
  System.get_env("ENABLE_TESTCONTAINERS") == "true" or
    Enum.any?(System.argv(), &(&1 in ["--only", "--include"] or String.contains?(&1, "chaos"))) or
    System.argv()
    |> Enum.chunk_every(2, 1, :discard)
    |> Enum.any?(fn
      ["--only", tag] -> tag == "chaos"
      ["--include", tag] -> tag == "chaos"
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
