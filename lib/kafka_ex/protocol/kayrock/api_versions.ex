defmodule KafkaEx.Protocol.Kayrock.ApiVersions do
  @moduledoc """
  This module handles ApiVersions request & response parsing.

  ApiVersions is a special API used during client initialization to discover
  which Kafka API versions the broker supports. This enables dynamic version
  negotiation for all other Kafka APIs.

  ## Supported Versions

  - **V0**: Basic version list (no throttle_time_ms)
  - **V1**: Adds throttle_time_ms
  - **V2**: Same schema as V1
  - **V3**: Flexible version (KIP-482) with client_software_name/version fields

  ## Forward Compatibility

  Both Request and Response protocols use `@fallback_to_any true` so that
  future Kayrock versions (V4+) are handled automatically without requiring
  explicit implementations.
  """

  alias KafkaEx.Client.Error
  alias KafkaEx.Messages.ApiVersions

  defprotocol Request do
    @moduledoc """
    This protocol is used to build ApiVersions requests.

    For versions without a specific implementation, falls back to the
    `Any` implementation which returns the request struct unchanged
    (ApiVersions requests are parameterless for V0-V2, and V3+ has
    client software fields handled by explicit impls).
    """

    @fallback_to_any true

    @spec build_request(t(), Keyword.t()) :: t()
    def build_request(request, opts)
  end

  defprotocol Response do
    @moduledoc """
    This protocol is used to parse ApiVersions responses.

    For versions without a specific implementation, falls back to the
    `Any` implementation which delegates to `ResponseHelpers.parse/1`.
    This handles all V1+ responses that include throttle_time_ms.
    """

    @fallback_to_any true

    @spec parse_response(t()) :: {:ok, ApiVersions.t()} | {:error, Error.t()}
    def parse_response(response)
  end
end
