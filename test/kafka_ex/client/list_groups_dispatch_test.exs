defmodule KafkaEx.Client.ListGroupsDispatchTest do
  @moduledoc """
  Regression for the ListGroups final-review Critical finding: a non-zero
  top-level `error_code` in a REAL ListGroups response must flow through
  the client retry loop as a `%KafkaEx.Client.Error{}`, not a bare
  atom.

  Pre-fix, `ResponseHelpers.parse_response/1` returned `{:error, error_atom}`
  for a non-zero `error_code`. The retry loop only matches
  `{:error, [error | _]}` or `{:error, %Error{} = error}`, so a bare atom hit
  no clause and raised `CaseClauseError`, crashing the client `GenServer`.

  This test drives the REAL pipeline: the fake broker answers with actual wire
  bytes for a `Kayrock.ListGroups.V0.Response{error_code: 15}`, which are
  deserialized by the real Kayrock/KayrockProtocol stack and parsed by the
  real `ResponseHelpers.parse_response/1` before reaching the retry loop --
  unlike the response/response_helpers unit tests, which call the parser
  directly and would not have caught this.
  """
  use KafkaEx.TestSupport.PinCase, async: false

  alias KafkaEx.Client.Error

  @moduletag pin_set: :invariant

  test "a non-zero top-level error_code from the broker returns %Error{} without crashing" do
    %{fake: fake, client: client} = start_pinned(api_versions: %{list_groups: 0..0})
    FakeTransport.respond(fake, :list_groups, &{:frame, <<&1.correlation_id::32-signed, 15::16-signed, 0::32-signed>>})

    # Pre-fix the bare atom crashed the client with a CaseClauseError, exiting this call.
    {result, _log} = with_log(fn -> Client.request(client, {:list_groups, 1, [api_version: 0]}, 5_000) end)

    assert {:error, %Error{error: :coordinator_not_available}} = result
    assert Process.alive?(client)
  end
end
