defmodule KafkaEx.TestSupport.TLSFixtures do
  @moduledoc false

  def chain do
    %{server_config: server, client_config: client} =
      :public_key.pkix_test_data(%{
        server_chain: %{root: [key: {:rsa, 2048, 65_537}], peer: [key: {:rsa, 2048, 65_537}, extensions: san_ext()]},
        client_chain: %{root: [key: {:rsa, 2048, 65_537}], peer: [key: {:rsa, 2048, 65_537}]}
      })

    %{server_config: _, client_config: other_client} =
      :public_key.pkix_test_data(%{
        server_chain: %{root: [key: {:rsa, 2048, 65_537}], peer: [key: {:rsa, 2048, 65_537}]},
        client_chain: %{root: [key: {:rsa, 2048, 65_537}], peer: [key: {:rsa, 2048, 65_537}]}
      })

    base = [verify: :verify_peer, server_name_indication: ~c"localhost"]

    %{
      server: Keyword.merge(server, fail_if_no_peer_cert: false, verify: :verify_none),
      client_ok: base ++ Keyword.take(client, [:cacerts]),
      client_wrong_ca: base ++ Keyword.take(other_client, [:cacerts])
    }
  end

  defp san_ext, do: [{:Extension, {2, 5, 29, 17}, false, [{:dNSName, ~c"localhost"}]}]

  # SSLServer dies on a failed handshake; this one keeps accepting.
  def start_acceptor(server_opts) do
    {:ok, listen} = :ssl.listen(0, [:binary, active: false, reuseaddr: true] ++ server_opts)
    {:ok, {_, port}} = :ssl.sockname(listen)
    pid = spawn_link(fn -> accept_loop(listen) end)
    :ok = :ssl.controlling_process(listen, pid)
    {:ok, port, fn -> Process.exit(pid, :kill) && :ok end}
  end

  defp accept_loop(listen) do
    case :ssl.transport_accept(listen) do
      {:ok, conn} ->
        _ = :ssl.handshake(conn, 5_000)
        accept_loop(listen)

      {:error, _} ->
        :ok
    end
  end
end
