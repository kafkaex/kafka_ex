defmodule KafkaExBench.Toxiproxy do
  @proxies ~w(kafka1_ssl kafka2_ssl kafka3_ssl kafka1_scram kafka2_scram kafka3_scram)
  @http_options [timeout: 5_000, connect_timeout: 2_000]

  def set_latency!(ms) do
    reset!()

    toxic = %{
      name: "latency",
      type: "latency",
      stream: "downstream",
      toxicity: 1.0,
      attributes: %{latency: ms, jitter: 0}
    }

    Enum.each(@proxies, fn proxy ->
      ensure_status!(post("/proxies/#{proxy}/toxics", toxic), 200, proxy)
    end)

    Enum.each(@proxies, fn proxy ->
      verify_latency!(proxy, get_json!("/proxies/#{proxy}/toxics"), ms)
    end)
  end

  def reset! do
    ensure_status!(post("/reset", %{}), 204, "reset")
  end

  def version!, do: parse_version(get!("/version"))

  def parse_version(body) do
    trimmed = String.trim(body)

    case Jason.decode(trimmed) do
      {:ok, %{"version" => version}} when is_binary(version) -> version
      {:ok, other} when is_map(other) -> raise "toxiproxy /version has no version field: #{trimmed}"
      _ -> trimmed
    end
  end

  def ensure_get_status!({:ok, {{_, 200, _}, _, body}}, _path), do: body

  def ensure_get_status!({:ok, {{_, status, _}, _, _}}, path) do
    raise "toxiproxy GET #{path} returned status #{status}"
  end

  def ensure_get_status!(error, path), do: raise("toxiproxy GET #{path} failed: #{inspect(error)}")

  def verify_latency!(proxy, toxics, ms) do
    case toxics do
      [%{"name" => "latency", "attributes" => %{"latency" => ^ms}}] ->
        :ok

      other ->
        raise "proxy #{proxy} should have exactly one latency toxic of #{ms} ms, has: #{inspect(other)}"
    end
  end

  def ensure_status!({:ok, status}, status, _proxy), do: :ok

  def ensure_status!(result, expected, proxy) do
    raise "toxiproxy #{proxy}: expected status #{expected}, got #{inspect(result)}"
  end

  defp get!(path) do
    ensure_get_status!(:httpc.request(:get, {url(path), []}, @http_options, body_format: :binary), path)
  end

  defp get_json!(path), do: path |> get!() |> Jason.decode!()

  defp post(path, body) do
    request = {url(path), [], ~c"application/json", Jason.encode!(body)}

    case :httpc.request(:post, request, @http_options, []) do
      {:ok, {{_, status, _}, _, _}} -> {:ok, status}
      error -> error
    end
  end

  defp url(path), do: String.to_charlist("http://localhost:8474" <> path)
end
