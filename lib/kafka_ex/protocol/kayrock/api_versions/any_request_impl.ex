defimpl KafkaEx.Protocol.Kayrock.ApiVersions.Request, for: Any do
  @moduledoc """
  Fallback implementation of ApiVersions Request protocol.

  ApiVersions requests are parameterless for V0-V2, and V3 has its own
  explicit implementation for client_software_name/version fields.

  Future versions (V4+) that don't have explicit implementations will
  fall through to this passthrough, which returns the request struct unchanged.
  """

  @doc """
  Returns the request struct unchanged.

  ApiVersions requests have no user-configurable parameters in their base form.
  Version-specific fields (like V3's client_software_name) are handled by
  explicit version implementations.
  """
  def build_request(request, _opts) do
    request
  end
end
