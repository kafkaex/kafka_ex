defmodule KafkaEx.Server0P9P0.Test do
  alias KafkaEx.Protocol, as: Proto
  alias KafkaEx.Config
  use ExUnit.Case
  import TestHelper

  @moduletag :server_0_p_9_p_0

  test "can join a consumer group" do
    random_group = generate_random_string
    KafkaEx.create_worker(:join_group, [uris: uris, consumer_group: random_group])

    # No wrapper in kafka_ex yet as long as the 0.9 functionality is in progress
    answer = GenServer.call(:join_group, {:join_group, ["foo", "bar"], 6000})
    assert answer.error_code == :no_error
    assert answer.generation_id == 1
    # We should be the leader
    assert answer.member_id == answer.leader_id
  end

  test "can send a simple leader sync for a consumer group" do
    # A lot of repetition with the previous test. Leaving it in now, waiting for
    # how this pans out eventually as we add more and more 0.9 consumer group code
    random_group = generate_random_string
    KafkaEx.create_worker(:sync_group, [uris: uris, consumer_group: random_group])
    answer = GenServer.call(:sync_group, {:join_group, ["foo", "bar"], 6000})
    assert answer.error_code == :no_error

    member_id = answer.member_id
    generation_id = answer.generation_id
    my_assignments = [{"foo", [1]}, {"bar", [2]}]
    assignments = [{member_id, my_assignments}]

    answer = GenServer.call(:sync_group, {:sync_group, random_group, generation_id, member_id, assignments})
    assert answer.error_code == :no_error
    # Parsing happens to return the assignments reversed, which is fine as there's no
    # ordering. Just reverse what we expect to match
    assert answer.assignments == Enum.reverse(my_assignments)
  end

  test "can heartbeat" do
    # See sync test. Removing repetition in the next iteration
    random_group = generate_random_string
    KafkaEx.create_worker(:heartbeat, [uris: uris, consumer_group: random_group])
    answer = GenServer.call(:heartbeat, {:join_group, ["foo", "bar"], 6000})
    assert answer.error_code == :no_error

    member_id = answer.member_id
    generation_id = answer.generation_id
    my_assignments = [{"foo", [1]}, {"bar", [2]}]
    assignments = [{member_id, my_assignments}]

    answer = GenServer.call(:heartbeat, {:sync_group, random_group, generation_id, member_id, assignments})
    assert answer.error_code == :no_error

    answer = GenServer.call(:heartbeat, {:heartbeat, random_group, generation_id, member_id})
    assert answer.error_code == :no_error
  end
end
