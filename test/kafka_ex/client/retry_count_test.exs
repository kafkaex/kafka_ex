defmodule KafkaEx.Client.RetryCountTest do
  @moduledoc """
  Regression for #357: `KafkaEx.Client.retry_count/0` is the single source of
  the request retry budget, which `KafkaEx.API` derives its fetch `call_timeout`
  from (`@fetch_max_retries`). The two were previously two literals kept equal by
  hand; the accessor + derivation make divergence impossible.
  """
  use ExUnit.Case, async: true

  test "retry_count/0 exposes the shared retry budget" do
    assert KafkaEx.Client.retry_count() == 3
  end
end
