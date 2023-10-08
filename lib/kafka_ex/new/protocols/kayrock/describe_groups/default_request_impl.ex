defimpl KafkaEx.New.Protocols.Kayrock.DescribeGroups.Request,
  for: [Kayrock.DescribeGroups.V0.Request, Kayrock.DescribeGroups.V1.Request] do
  def build_request(request_template, consumer_group_names) do
    Map.put(request_template, :group_ids, consumer_group_names)
  end
end
