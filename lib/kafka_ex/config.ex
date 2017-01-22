defmodule KafkaEx.Config do
  @moduledoc """
  Configuring KafkaEx

  ```
  """ <> File.read!(Path.expand("../../config/config.exs", __DIR__)) <> """
  ```
  """

  require Logger

  @doc false
  def use_ssl, do: Application.get_env(:kafka_ex, :use_ssl)

  # use this function to get the ssl options - it verifies the options and
  #   either emits warnings or raises errors as appropriate on misconfiguration
  @doc false
  def ssl_options do
    ssl_options(use_ssl(), Application.get_env(:kafka_ex, :ssl_options))
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

  defp server("0.8.0"), do: KafkaEx.Server0P8P0
  defp server("0.8.2"), do: KafkaEx.Server0P8P2
  defp server(_), do: KafkaEx.Server0P9P0

  # ssl_options should be an empty list by default if use_ssl is false
  defp ssl_options(false, []), do: []
  defp ssl_options(false, nil), do: []
  # emit a warning if use_ssl is false but options are present
  #   (this is not a fatal error and can occur if one disables ssl in the
  #    default option set)
  defp ssl_options(false, options) do
    Logger.warn("Ignoring ssl_options #{inspect options} because " <>
      "use_ssl is false.  If you do not intend to use ssl and want to " <>
      "remove this warning, set `ssl_options: []` in the KafkaEx config.")
    []
  end
  # if ssl is enabled, verify that cacertfile, certfile, and keyfile are set
  # and point to readable files
  defp ssl_options(true, options) do
    options
    |> verify_ssl_file(:cacertfile)
    |> verify_ssl_file(:certfile)
    |> verify_ssl_file(:keyfile)
  end

  defp verify_ssl_file(options, key) do
    verify_ssl_file(options, key, Keyword.get(options, key))
  end

  defp verify_ssl_file(options, _key, nil) do
    # cert file not present - it will be up to :ssl to determine if this is ok
    # given the other settings
    options
  end
  defp verify_ssl_file(options, key, path) do
    # make sure the file is readable to us
    #    (there is a way to do this without reading the file, but these should
    #      be small files so this is probably the simplest option)
    case File.read(path) do
      {:ok, _} -> options
      {:error, reason} ->
        raise(
          ArgumentError,
          message: "SSL option #{inspect key} could not be read.  " <>
            "Error: #{inspect reason}"
        )
    end
  end
end
