defimpl KafkaEx.Protocol.Kayrock.Metadata.Response, for: Kayrock.Metadata.V13.Response do
  @moduledoc """
  Implementation of Metadata Response protocol for Kafka V13 API.

  V13 adds a top-level `error_code` (KIP-1102, e.g. `REBOOTSTRAP_REQUIRED`).
  A non-zero value is returned as `{:error, %KafkaEx.Client.Error{}}`; on
  success parsing is the same as V12.
  """

  alias KafkaEx.Protocol.Kayrock.Metadata.ResponseHelpers

  def parse_response(response) do
    ResponseHelpers.to_cluster_metadata_with_top_level_error(response)
  end
end
