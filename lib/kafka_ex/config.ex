defmodule KafkaEx.Config do
  @moduledoc """
             Configuring KafkaEx

             ```
             """ <>
               File.read!(Path.expand("../../config/config.exs", __DIR__)) <>
               """
               ```
               """

  require Logger

  @doc false
  def disable_default_worker do
    Application.get_env(:kafka_ex, :disable_default_worker, false)
  end

  @doc false
  def client_id do
    Application.get_env(:kafka_ex, :client_id, "kafka_ex")
  end

  @doc false
  def consumer_group do
    Application.get_env(:kafka_ex, :consumer_group, "kafka_ex")
  end

  @doc false
  def use_ssl, do: Application.get_env(:kafka_ex, :use_ssl, false)

  # use this function to get the ssl options - it verifies the options and
  #   either emits warnings or raises errors as appropriate on misconfiguration
  @doc false
  def ssl_options do
    ssl_options(use_ssl(), Application.get_env(:kafka_ex, :ssl_options, []))
  end

  @doc false
  def default_worker do
    :kafka_ex
  end

  @doc false
  def server_impl do
    :kafka_ex
    |> Application.get_env(:kafka_version, :default)
    |> server
  end

  @doc false
  def brokers do
    :kafka_ex
    |> Application.get_env(:brokers)
    |> brokers()
  end

  defp brokers(nil),
    do: nil

  defp brokers(list) when is_list(list),
    do: list

  defp brokers(csv) when is_binary(csv) do
    for line <- String.split(csv, ","), into: [] do
      case line |> trim() |> String.split(":") do
        [host] ->
          msg = "Port not set for Kafka broker #{host}"
          Logger.warning(msg)
          raise msg

        [host, port] ->
          {port, _} = Integer.parse(port)
          {host, port}
      end
    end
  end

  defp brokers({mod, fun, args}) when is_atom(mod) and is_atom(fun) do
    apply(mod, fun, args)
  end

  defp brokers(fun) when is_function(fun, 0) do
    fun.()
  end

  if Version.match?(System.version(), "<1.3.0") do
    defp trim(string), do: String.strip(string)
  else
    defp trim(string), do: String.trim(string)
  end

  defp server("0.8.0"), do: KafkaEx.Server0P8P0
  defp server("0.8.2"), do: KafkaEx.Server0P8P2
  defp server("0.9.0"), do: KafkaEx.Server0P9P0
  defp server("kayrock"), do: KafkaEx.New.Client
  defp server(_), do: KafkaEx.Server0P10AndLater

  # ssl_options should be an empty list by default if use_ssl is false
  defp ssl_options(false, []), do: []
  # emit a warning if use_ssl is false but options are present
  #   (this is not a fatal error and can occur if one disables ssl in the
  #    default option set)
  defp ssl_options(false, options) do
    Logger.warning(
      "Ignoring ssl_options #{inspect(options)} because " <>
        "use_ssl is false.  If you do not intend to use ssl and want to " <>
        "remove this warning, set `ssl_options: []` in the KafkaEx config."
    )

    []
  end

  # verify that options is at least a keyword list
  defp ssl_options(true, options) do
    if Keyword.keyword?(options) do
      options
    else
      raise(
        ArgumentError,
        "SSL is enabled and invalid ssl_options were provided: " <>
          inspect(options)
      )
    end
  end
end
