defmodule KafkaEx.Client.RequestContextTest.FakeRequest do
  @moduledoc false
  defstruct [:id]
end

defmodule KafkaEx.Client.RequestContextTest do
  use ExUnit.Case, async: true

  alias KafkaEx.Client.NodeSelector
  alias KafkaEx.Client.RequestContext
  alias KafkaEx.Client.RequestContextTest.FakeRequest

  describe "struct defaults" do
    test "retryable? defaults to the data-plane classifier (transient/leadership retry, fatal fail-fast); network_timeout nil" do
      ctx = %RequestContext{
        request: %FakeRequest{},
        parser_fn: fn _ -> {:ok, :parsed} end,
        node_selector: NodeSelector.first_available()
      }

      assert ctx.retryable?.(:not_leader_for_partition) == true
      assert ctx.retryable?.(:timeout) == true
      assert ctx.retryable?.(:topic_authorization_failed) == false
      assert ctx.network_timeout == nil
    end

    test "enforces required keys" do
      assert_raise ArgumentError, fn ->
        struct!(RequestContext, parser_fn: fn _ -> :ok end)
      end
    end
  end
end
