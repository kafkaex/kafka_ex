defimpl KafkaEx.Protocol.Kayrock.ApiVersions.Request, for: Kayrock.ApiVersions.V3.Request do
  @moduledoc """
  Implements ApiVersions V3 request building.

  V3 is a flexible version (KIP-482) that introduces:
  - `client_software_name` (compact_string): identifies the client library
  - `client_software_version` (compact_string): identifies the client library version
  - `tagged_fields`: for forward-compatible extensibility

  These fields allow brokers to log which client software is connecting,
  useful for diagnostics and monitoring.
  """

  @default_client_software_name "kafka_ex"
  @default_client_software_version Mix.Project.config()[:version] || "0.0.0"

  @doc """
  Builds an ApiVersions V3 request.

  ## Options

  - `:client_software_name` - Name of the client software (default: "kafka_ex")
  - `:client_software_version` - Version of the client software
    (default: KafkaEx version from mix.exs, currently #{@default_client_software_version})
  """
  @spec build_request(Kayrock.ApiVersions.V3.Request.t(), Keyword.t()) ::
          Kayrock.ApiVersions.V3.Request.t()
  def build_request(request, opts) do
    client_name = Keyword.get(opts, :client_software_name, @default_client_software_name)
    client_version = Keyword.get(opts, :client_software_version, @default_client_software_version)

    %{request | client_software_name: client_name, client_software_version: client_version}
  end
end
