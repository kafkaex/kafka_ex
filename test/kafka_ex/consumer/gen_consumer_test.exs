defmodule KafkaEx.Consumer.GenConsumerTest do
  use ExUnit.Case
  import ExUnit.CaptureLog

  # non-integration GenConsumer tests

  defmodule TestConsumer do
    use KafkaEx.Consumer.GenConsumer

    def handle_message_set(_, state), do: state
  end

  test "calling handle_call raises an error if there is no implementation" do
    assert_raise RuntimeError, fn -> TestConsumer.handle_call(nil, nil, nil) end
  end

  test "calling handle_cast raises an error if there is no implementation" do
    assert_raise RuntimeError, fn -> TestConsumer.handle_cast(nil, nil) end
  end

  test "calling handle_info raises an error if there is no implementation" do
    assert capture_log(fn ->
             TestConsumer.handle_info(nil, nil)
           end) =~ "unexpected message in handle_info"
  end

  describe "build_fetch_options/1 (used at init to fold :client_rack into fetch_options)" do
    alias KafkaEx.Consumer.GenConsumer

    test "preserves the default auto_commit: false" do
      assert GenConsumer.build_fetch_options([])[:auto_commit] == false
    end

    test ":client_rack is folded in as :rack_id" do
      opts = [client_rack: "az-a"]
      assert GenConsumer.build_fetch_options(opts)[:rack_id] == "az-a"
    end

    test "no :rack_id is set when :client_rack is absent" do
      refute Keyword.has_key?(GenConsumer.build_fetch_options([]), :rack_id)
    end

    test "explicit fetch_options[:rack_id] wins over :client_rack" do
      opts = [client_rack: "az-a", fetch_options: [rack_id: "az-explicit"]]
      assert GenConsumer.build_fetch_options(opts)[:rack_id] == "az-explicit"
    end

    test "user-supplied fetch_options are merged with defaults" do
      opts = [fetch_options: [max_bytes: 1024]]
      result = GenConsumer.build_fetch_options(opts)
      assert result[:auto_commit] == false
      assert result[:max_bytes] == 1024
    end
  end
end
