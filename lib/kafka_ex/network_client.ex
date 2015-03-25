defmodule KafkaEx.NetworkClient do
  def new(client_id) do
    %{client_id: client_id, correlation_id: 0, hosts: %{}}
  end

  def shutdown(client) do
    Enum.each(Map.values(client.hosts), fn(socket) ->
      if socket, do: :gen_tcp.close(socket)
    end)
  end

  def send_request(client, host_list, request, timeout \\ 100)

  def send_request(client, [{host, port}|rest], request_fn, timeout) do
    request = request_fn.(client.correlation_id, client.client_id)
    case send_to_host(client, host, port, request, timeout) do
      {:error, reason} ->
        case rest do
          [] -> raise "Error sending request: #{reason}"
          _  -> send_request(rest, request, timeout)
        end
      client -> get_response(%{client | correlation_id: client.correlation_id + 1}, timeout)
    end
  end

  def update_from_metadata(client, host_list) do
    from_metadata = Enum.into(host_list, HashSet.new)
    current_hosts = Enum.into(Map.keys(client.hosts), HashSet.new)
    if HashSet.equal?(from_metadata, current_hosts) do
      client
    else
      removing = HashSet.difference(current_hosts, from_metadata)
      {to_remove, keep} = Map.split(client.hosts, removing)

      Enum.each(to_remove,
        fn(kv) ->
          socket = elem(kv, 1)
          if socket != nil do
            :gen_tcp.close(socket)
          end
        end)

      adding = HashSet.difference(from_metadata, current_hosts)
      updated = Enum.reduce(adding, keep, fn(host, map) -> Map.put(map, host, nil) end)
      %{client | hosts: updated}
    end
  end

  defp send_to_host(client, host, port, request, timeout) when is_list(port) do
    send_to_host(client, host, to_string(port), request, timeout)
  end

  defp send_to_host(client, host, port, request, timeout) when is_binary(port) do
    send_to_host(client, host, String.to_integer(port), request, timeout)
  end

  defp send_to_host(client, host, port, request, timeout) when is_list(host) do
    send_to_host(client, to_string(host), port, request, timeout)
  end

  defp send_to_host(client, host, port, request, timeout) when is_binary(host) and is_integer(port) do
    case get_socket(client, host, port) do
      {:error, reason} -> {:error, reason}
      {client, socket} ->
        case do_send(socket, request, timeout) do
          :ok -> client
          error -> error
        end
    end
  end

  defp get_socket(client, host, port) do
    if Map.has_key?(client.hosts, {host, port}) && client.hosts[{host, port}] do
      {client, client.hosts[{host, port}]}
    else
      case :gen_tcp.connect(format_host(host), port, [:binary, {:packet, 4}]) do
        {:ok, socket} ->
          {%{client | hosts: Map.put_new(client.hosts, {host, port}, socket)}, socket}
        other -> other
      end
    end
  end

  def format_host(host) do
    case Regex.scan(~r/\d+/, host) do
      match_data = [_, _, _, _] -> match_data |> List.flatten |> Enum.map(&String.to_integer/1) |> List.to_tuple
      _ -> to_char_list(host)
    end
  end

  defp do_send(socket, request, timeout) do
    :gen_tcp.send(socket, request)
  end

  defp get_response(client, 0) do
    {client, nil}
  end

  defp get_response(client, timeout) do
    receive do
      {:tcp, _, data} -> {client, data}
    after
      timeout -> {client, {:error, :timeout}}
    end
  end
end
