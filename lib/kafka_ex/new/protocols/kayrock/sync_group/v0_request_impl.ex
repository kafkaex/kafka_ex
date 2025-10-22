defimpl KafkaEx.New.Protocols.Kayrock.SyncGroup.Request, for: Kayrock.SyncGroup.V0.Request do
  @moduledoc """
  Implementation for SyncGroup v0 Request.
  """

  alias KafkaEx.New.Protocols.Kayrock.SyncGroup.RequestHelpers

  def build_request(request_template, opts) do
    RequestHelpers.build_request_from_template(request_template, opts)
  end
end
