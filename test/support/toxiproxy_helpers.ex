defmodule KafkaEx.ToxiproxyHelpers do
  @moduledoc """
  Helper functions for chaos engineering with Toxiproxy.

  Provides convenience wrappers around Toxiproxy's HTTP API for adding
  network toxics (latency, bandwidth limits, connection drops, etc.) to test
  KafkaEx's resilience.

  ## Usage

      # In your test setup
      {:ok, proxy_port} = ToxiproxyContainer.create_proxy_for_container(
        toxiproxy, "kafka_proxy", kafka, 9092
      )

      # Add chaos
      add_latency(toxiproxy, "kafka_proxy", 1000)  # 1 second latency

      # Run test logic

      # Clean up
      remove_all_toxics(toxiproxy, "kafka_proxy")

  ## Available Toxics

  - **latency** - Add delay to all data passing through proxy
  - **bandwidth** - Limit bandwidth (rate limit in KB/s)
  - **slow_close** - Delay connection termination
  - **timeout** - Stop all data after timeout and close connection
  - **slicer** - Slice data into smaller packets
  - **limit_data** - Close connection after transmitting N bytes
  - **down** - Take connection offline (0% availability)
  - **reset_peer** - Simulate TCP RST (connection reset)
  """

  alias Testcontainers.ToxiproxyContainer

  @doc """
  Adds latency toxic to a proxy.

  ## Parameters

    - `container` - The Toxiproxy container
    - `proxy_name` - Name of the proxy to affect
    - `latency_ms` - Latency in milliseconds to add
    - `jitter_ms` - Optional jitter in milliseconds (default: 0)
  """
  def add_latency(container, proxy_name, latency_ms, jitter_ms \\ 0) do
    toxic = %{
      type: "latency",
      name: "latency_downstream",
      stream: "downstream",
      toxicity: 1.0,
      attributes: %{
        latency: latency_ms,
        jitter: jitter_ms
      }
    }

    add_toxic(container, proxy_name, toxic)
  end

  @doc """
  Adds upstream latency toxic to a proxy.

  Like `add_latency/4` but affects upstream (client to server) traffic.
  """
  def add_upstream_latency(container, proxy_name, latency_ms, jitter_ms \\ 0) do
    toxic = %{
      type: "latency",
      name: "latency_upstream",
      stream: "upstream",
      toxicity: 1.0,
      attributes: %{
        latency: latency_ms,
        jitter: jitter_ms
      }
    }

    add_toxic(container, proxy_name, toxic)
  end

  @doc """
  Limits bandwidth through the proxy.

  ## Parameters

    - `container` - The Toxiproxy container
    - `proxy_name` - Name of the proxy
    - `rate_kbps` - Bandwidth limit in kilobytes per second
  """
  def add_bandwidth_limit(container, proxy_name, rate_kbps) do
    toxic = %{
      type: "bandwidth",
      name: "bandwidth_downstream",
      stream: "downstream",
      toxicity: 1.0,
      attributes: %{
        rate: rate_kbps
      }
    }

    add_toxic(container, proxy_name, toxic)
  end

  @doc """
  Disables a proxy entirely (makes it unavailable, connection refused).

  This is equivalent to `ToxiproxyEx.down!` — it sets `enabled: false` on
  the proxy via the Toxiproxy API rather than adding a toxic.

  ## Parameters

    - `container` - The Toxiproxy container
    - `proxy_name` - Name of the proxy
  """
  def disable_proxy(container, proxy_name) do
    host = Testcontainers.get_host()
    api_port = ToxiproxyContainer.mapped_control_port(container)

    :inets.start()

    url = ~c"http://#{host}:#{api_port}/proxies/#{proxy_name}"
    body = Jason.encode!(%{enabled: false})
    headers = [{~c"content-type", ~c"application/json"}]

    case :httpc.request(:post, {url, headers, ~c"application/json", body}, [], []) do
      {:ok, {{_, 200, _}, _, _}} -> :ok
      {:ok, {{_, code, _}, _, resp_body}} -> {:error, {:http_error, code, resp_body}}
      {:error, reason} -> {:error, reason}
    end
  end

  @doc """
  Re-enables a previously disabled proxy.

  ## Parameters

    - `container` - The Toxiproxy container
    - `proxy_name` - Name of the proxy
  """
  def enable_proxy(container, proxy_name) do
    host = Testcontainers.get_host()
    api_port = ToxiproxyContainer.mapped_control_port(container)

    :inets.start()

    url = ~c"http://#{host}:#{api_port}/proxies/#{proxy_name}"
    body = Jason.encode!(%{enabled: true})
    headers = [{~c"content-type", ~c"application/json"}]

    case :httpc.request(:post, {url, headers, ~c"application/json", body}, [], []) do
      {:ok, {{_, 200, _}, _, _}} -> :ok
      {:ok, {{_, code, _}, _, resp_body}} -> {:error, {:http_error, code, resp_body}}
      {:error, reason} -> {:error, reason}
    end
  end

  @doc """
  Delays connection close.

  ## Parameters

    - `container` - The Toxiproxy container
    - `proxy_name` - Name of the proxy
    - `delay_ms` - Delay in milliseconds before closing
  """
  def add_slow_close(container, proxy_name, delay_ms) do
    toxic = %{
      type: "slow_close",
      name: "slow_close_downstream",
      stream: "downstream",
      toxicity: 1.0,
      attributes: %{
        delay: delay_ms
      }
    }

    add_toxic(container, proxy_name, toxic)
  end

  @doc """
  Stops all data after timeout and closes connection.

  ## Parameters

    - `container` - The Toxiproxy container
    - `proxy_name` - Name of the proxy
    - `timeout_ms` - Timeout in milliseconds
  """
  def add_timeout(container, proxy_name, timeout_ms) do
    toxic = %{
      type: "timeout",
      name: "timeout_downstream",
      stream: "downstream",
      toxicity: 1.0,
      attributes: %{
        timeout: timeout_ms
      }
    }

    add_toxic(container, proxy_name, toxic)
  end

  @doc """
  Simulates TCP RST (connection reset by peer).

  ## Parameters

    - `container` - The Toxiproxy container
    - `proxy_name` - Name of the proxy
    - `timeout_ms` - Milliseconds before the connection is reset
  """
  def add_reset_peer(container, proxy_name, timeout_ms) do
    toxic = %{
      type: "reset_peer",
      name: "reset_peer_downstream",
      stream: "downstream",
      toxicity: 1.0,
      attributes: %{
        timeout: timeout_ms
      }
    }

    add_toxic(container, proxy_name, toxic)
  end

  @doc """
  Slices data into smaller packets to simulate packet fragmentation.

  ## Parameters

    - `container` - The Toxiproxy container
    - `proxy_name` - Name of the proxy
    - `average_size` - Average packet size in bytes
    - `size_variation` - Size variation in bytes
    - `delay_us` - Delay between packets in microseconds
  """
  def add_slicer(container, proxy_name, average_size, size_variation, delay_us) do
    toxic = %{
      type: "slicer",
      name: "slicer_downstream",
      stream: "downstream",
      toxicity: 1.0,
      attributes: %{
        average_size: average_size,
        size_variation: size_variation,
        delay: delay_us
      }
    }

    add_toxic(container, proxy_name, toxic)
  end

  @doc """
  Closes connection after transmitting N bytes.

  ## Parameters

    - `container` - The Toxiproxy container
    - `proxy_name` - Name of the proxy
    - `bytes` - Number of bytes to transmit before closing
  """
  def add_limit_data(container, proxy_name, bytes) do
    toxic = %{
      type: "limit_data",
      name: "limit_data_downstream",
      stream: "downstream",
      toxicity: 1.0,
      attributes: %{
        bytes: bytes
      }
    }

    add_toxic(container, proxy_name, toxic)
  end

  @doc """
  Removes a specific toxic from a proxy.

  ## Parameters

    - `container` - The Toxiproxy container
    - `proxy_name` - Name of the proxy
    - `toxic_name` - Name of the toxic to remove
  """
  def remove_toxic(container, proxy_name, toxic_name) do
    host = Testcontainers.get_host()
    api_port = ToxiproxyContainer.mapped_control_port(container)

    :inets.start()

    url = ~c"http://#{host}:#{api_port}/proxies/#{proxy_name}/toxics/#{toxic_name}"

    case :httpc.request(:delete, {url, []}, [], []) do
      {:ok, {{_, 204, _}, _, _}} -> :ok
      {:ok, {{_, 404, _}, _, _}} -> {:error, :not_found}
      {:ok, {{_, code, _}, _, body}} -> {:error, {:http_error, code, body}}
      {:error, reason} -> {:error, reason}
    end
  end

  @doc """
  Removes all toxics from a proxy.

  This is useful for cleanup between test scenarios.

  ## Parameters

    - `container` - The Toxiproxy container
    - `proxy_name` - Name of the proxy
  """
  def remove_all_toxics(container, proxy_name) do
    case list_toxics(container, proxy_name) do
      {:ok, toxics} ->
        Enum.each(toxics, fn toxic ->
          remove_toxic(container, proxy_name, toxic["name"])
        end)

        :ok

      error ->
        error
    end
  end

  @doc """
  Resets all proxies: removes all active toxics and re-enables all proxies.

  Equivalent to `POST /reset` on the Toxiproxy API.
  """
  def reset_all_proxies(container) do
    host = Testcontainers.get_host()
    api_port = ToxiproxyContainer.mapped_control_port(container)

    :inets.start()

    url = ~c"http://#{host}:#{api_port}/reset"
    headers = [{~c"content-type", ~c"application/json"}]

    case :httpc.request(:post, {url, headers, ~c"application/json", ~c""}, [], []) do
      {:ok, {{_, 204, _}, _, _}} -> :ok
      {:ok, {{_, code, _}, _, body}} -> {:error, {:http_error, code, body}}
      {:error, reason} -> {:error, reason}
    end
  end

  @doc """
  Lists all toxics configured for a proxy.
  """
  def list_toxics(container, proxy_name) do
    host = Testcontainers.get_host()
    api_port = ToxiproxyContainer.mapped_control_port(container)

    :inets.start()

    url = ~c"http://#{host}:#{api_port}/proxies/#{proxy_name}/toxics"

    case :httpc.request(:get, {url, []}, [], []) do
      {:ok, {{_, 200, _}, _, body}} ->
        {:ok, Jason.decode!(to_string(body))}

      {:ok, {{_, code, _}, _, body}} ->
        {:error, {:http_error, code, body}}

      {:error, reason} ->
        {:error, reason}
    end
  end

  # Private helper to add a toxic via HTTP API
  defp add_toxic(container, proxy_name, toxic_config) do
    host = Testcontainers.get_host()
    api_port = ToxiproxyContainer.mapped_control_port(container)

    :inets.start()

    url = ~c"http://#{host}:#{api_port}/proxies/#{proxy_name}/toxics"
    body = Jason.encode!(toxic_config)
    headers = [{~c"content-type", ~c"application/json"}]

    case :httpc.request(:post, {url, headers, ~c"application/json", body}, [], []) do
      {:ok, {{_, code, _}, _, _}} when code in [200, 201] ->
        :ok

      {:ok, {{_, 409, _}, _, _}} ->
        # Toxic already exists
        {:error, :already_exists}

      {:ok, {{_, code, _}, _, response_body}} ->
        {:error, {:http_error, code, response_body}}

      {:error, reason} ->
        {:error, reason}
    end
  end
end
