defimpl KafkaEx.Protocol.Kayrock.Metadata.Request, for: Kayrock.Metadata.V11.Request do
  @moduledoc """
  Implementation of Metadata Request protocol for Kafka V11 API.

  V11 removes `include_cluster_authorized_operations` (now exposed by the
  DescribeCluster API, KIP-700). Topics are still sent by name only, with the
  null topic id, as in V10.
  """

  alias KafkaEx.Protocol.Kayrock.Metadata.RequestHelpers

  def build_request(request_template, opts) do
    RequestHelpers.build_v11_request(request_template, opts)
  end
end
