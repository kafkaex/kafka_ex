defimpl KafkaEx.Protocol.Kayrock.OffsetCommit.Request, for: Kayrock.OffsetCommit.V1.Request do
  @moduledoc """
  Implementation for OffsetCommit v1 Request.

  This version adds timestamp, generation_id, and member_id for consumer group coordination.
  Can store offsets in either Kafka or Zookeeper depending on configuration.
  """

  alias KafkaEx.Protocol.Kayrock.OffsetCommit.RequestHelpers

  def build_request(request_template, opts) do
    %{group_id: group_id} = RequestHelpers.extract_common_fields(opts)
    %{generation_id: generation_id, member_id: member_id} = RequestHelpers.extract_coordination_fields(opts)
    topics = RequestHelpers.build_topics(opts, true)

    request_template
    |> Map.put(:group_id, group_id)
    |> Map.put(:generation_id, generation_id)
    |> Map.put(:member_id, member_id)
    |> Map.put(:topics, topics)
  end
end
