defmodule FastCommands do
  @endpoint {"localhost", 9092}
  @client :kafka_client
  @topic "first_topic"
  @partition 0
  def start_all do
    KafkaConsumerPID.start_link()
    KafkaProducer.start_client()
    KafkaProducer.start_producer()
    KafkaConsumer.start()
  end

  def send_multiple_messages(qtd) when qtd <= 0, do: :brod.produce(@client, @topic, @partition, "fim", " fim do loop")
  def send_multiple_messages(qtd) do
    :brod.produce(@client, @topic, @partition, "#{qtd}", " estamos na posicao #{qtd} do loop")
    :timer.sleep(3000)
    FastCommands.send_multiple_messages(qtd- 1)

  end


  def message(value) when is_binary(value), do: KafkaProducer.send_message("generic key", value)
  def message(_), do: {:error, "invalid message"}

  def message(key, value) when is_binary(key) and is_binary(value), do: KafkaProducer.send_message(key, value)
  def message(_, _), do: {:error, "invalid key or message"}


  def create_topic(topic_name) when is_binary(topic_name), do: :brod.create_topics([@endpoint], [%{name: topic_name, num_partitions: 1, replication_factor: 1, assignments: [], configs: [ %{name: <<"cleanup.policy">>, value: "compact"}]}], %{timeout: 1000}, [])
  def create_topic(_), do: {:error, "invalid topic name"}

  def create_topic(topic_name, partitions) when is_binary(topic_name) and is_integer(partitions), do: :brod.create_topics([@endpoint], [%{name: topic_name, num_partitions: partitions, replication_factor: 1, assignments: [], configs: [ %{name: <<"cleanup.policy">>, value: "compact"}]}], %{timeout: 1000}, [])
  def create_topic(_, _), do: {:error, "invalid partitions or topic"}

  def create_topic(topic_name, partitions, replication) when is_binary(topic_name) and is_integer(partitions) and is_integer(replication), do: :brod.create_topics([@endpoint], [%{name: topic_name, num_partitions: partitions, replication_factor: replication, assignments: [], configs: [ %{name: <<"cleanup.policy">>, value: "compact"}]}], %{timeout: 1000}, [])
  def create_topic(_, _, _), do: {:error, "invalid topic, partitions or replication"}

  def delete_topic(topic_name) when is_binary(topic_name), do: :brod.delete_topics([@endpoint], [topic_name], 5000)
  def delete_topic(_), do: {:error, "invalid topic name"}


  def stop_all() do
    gspid = KafkaConsumerPID.get()
    :brod_group_subscriber.stop(gspid)
    :brod.stop_client(:kafka_client)
  end

end
