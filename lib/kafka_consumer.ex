defmodule KafkaConsumer do
  @behaviour :brod_group_consumer
  @client :kafka_client
  @topic "first_topic"

  def init(_arg, _) do
    {:ok, nil}
  end

  def start do
    :brod.start_link_group_subscriber(
    :kafka_client,
    "my_consumer_group",
    [@topic],
    [],
    [],
    KafkaConsumer,
    []
    )
    |>elem(1)
    |>KafkaConsumerPID.set()
  end

  def handle_message(_topic, _partition, message_set, state) when is_list(message_set) do
    for {:kafkamessage, _,_ , _, value,_ ,_ } <- message_set do
      IO.puts("Received message list: #{inspect(value)}")
    end
    {:ok, state}
  end

  def handle_message(topic, partition, {:kafka_message, offset, key, value, type, ts, headers}, state) do
    thred = spawn(fn ->
      IO.puts("Thread started")
      Process.sleep(1000)
      IO.puts("----------------------------------------------")
      IO.puts("Partition: #{inspect(partition)}")
      IO.puts("topic: #{inspect(topic)}")
      # IO.puts("Offset: #{inspect(offset)}")
      # IO.puts("Key: #{inspect(key)}")
      IO.puts("Status: #{inspect(type)}")
      IO.puts("Received message: #{inspect(value)}")
      # IO.puts("ts: #{inspect(ts)}")
      # IO.puts("Headers: #{inspect(headers)}")
      #IO.puts("State: #{inspect(state)}")
      IO.puts("----------------------------------------------")
      IO.puts("Thread ended")
    end)
    {:ok, state}
    #:brod_group_subscriber.commit(TUDO PEDE O PID AAAAAAA EU TO FICANDO MALUCOOO)
  end

  def handle_message(_topic, _partition, message_set, state) do
    for message <- message_set do
      IO.puts("Received message: #{inspect(message.value)}")
    end
    {:ok, state}
  end

  def handle_error(_topic, _partition, _error, reason, _committed_offsets, state) do
    IO.puts("Error processing message: #{reason}")
    {:ok, state}
  end

  def handle_assign(_assigns, _group_member_pid, state) do
    {:ok, state}
  end

  def handle_revoke(_partitions, _group_member_pid, state) do
    {:ok, state}
  end

  def handle_commit(_offsets, state) do
    {:ok, state}
  end

  def handle_leave(_group_member_pid, _reason, state) do
    {:ok, state}
  end

  def terminate(_reason, _state) do
    :ok
  end
end

defmodule KafkaConsumerPID do
  use Agent

  def start_link do
    Agent.start_link(fn -> nil end, name: __MODULE__)
  end

  def set(pid) do
    Agent.update(__MODULE__, fn _ -> pid end)
  end

  def get do
    Agent.get(__MODULE__, fn pid -> pid end)
  end
end
