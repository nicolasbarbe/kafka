package kafka

import ( 
        "github.com/Shopify/sarama"
        "log"
)

const (
  bufferSize     = 256
  initialOffset  = sarama.OffsetOldest // always start listening for the latest event. 
)

type Consumer struct {
  consumer           sarama.Consumer
  partitionConsumers []sarama.PartitionConsumer
  messages           chan *sarama.ConsumerMessage
}

func NewConsumer(brokers []string, topic string) *Consumer {
  config := sarama.NewConfig()
  config.Consumer.Return.Errors = true

  consumer, err := sarama.NewConsumer(brokers, config) 
  if err != nil {
    log.Fatalln(err)
  }

  partitions, err := consumer.Partitions(topic) 
  if err != nil {
    log.Printf("Failed to get the list of partitions: %s", err)
  }

  log.Printf("%d partitions found for topic %s", len(partitions), topic)

  partitionConsumers := make([]sarama.PartitionConsumer, len(partitions))
  messages := make(chan *sarama.ConsumerMessage, bufferSize)

  for _, partition := range partitions {

    partitionConsumer, err := consumer.ConsumePartition(topic, partition, initialOffset)

    if err != nil {
      log.Fatalf("Failed to start consumer for partition %d: %s", partition, err)
    }

    go func(partitionConsumer sarama.PartitionConsumer) {
      for message := range partitionConsumer.Messages() {
        messages <- message
      }
    }(partitionConsumer)

  }

  return &Consumer{ 
    consumer           : consumer, 
    partitionConsumers : partitionConsumers,
    messages           : messages,
  }
}

// Consume messages and process them through the method pass in parameter
func (this *Consumer) Consume(processMessage func(key, value []byte)) {
  go func() {
      log.Println("Start consuming messages ...")

      for message := range this.messages {
        log.Printf("Process message with offset %s", message.Offset)
        processMessage(message.Key, message.Value)
      }
    }()  
}


// Close stops processing messages and releases the corresponding resources
func (this *Consumer) Close() {

  log.Println("Done consuming messages")
 
  for _, partitionConsumer := range this.partitionConsumers {
    if err := partitionConsumer.Close(); err != nil {
      log.Printf("Failed to close partition consumer: ", err)
    }
  }

  if err := this.consumer.Close(); err != nil {
    log.Printf("Failed to shutdown kafka consumer cleanly: %s", err)
  }  

  close(this.messages)

}