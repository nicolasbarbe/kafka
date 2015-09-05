package kafka

import ( 
        "github.com/Shopify/sarama"
        "encoding/json" 
        "bytes"
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
  Registry           EventTypeRegistry
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
    log.Printf("Failed to get the list of partitions: %v", err)
  }

  log.Printf("%v partitions found for topic %v", len(partitions), topic)

  partitionConsumers := make([]sarama.PartitionConsumer, len(partitions))
  messages := make(chan *sarama.ConsumerMessage, bufferSize)

  for _, partition := range partitions {

    partitionConsumer, err := consumer.ConsumePartition(topic, partition, initialOffset)

    if err != nil {
      log.Fatalf("Failed to start consumer for partition %v: %v", partition, err)
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
    Registry           : NewEventTypeRegistry(),
  }
}

// Consume messages and process them through the method pass in parameter
func (this *Consumer) Consume(processEvent func(interface{})) {
  go func() {
      log.Println("Start consuming messages ...")

      for message := range this.messages {
        log.Printf("Process message with offset %v", message.Offset)

        b := bytes.SplitAfterN(message.Value[:], []byte {44}, 1)

        eventType  := string(b[0][:])
        event, err := this.Registry.GetType(eventType)
        if err != nil {
          log.Println("Cannot find event type %v in the registry", eventType)
          continue
        }

        if err := json.Unmarshal(b[1], event) ; err != nil {
          log.Println("Cannot read event : ", err)
          continue
        }

        processEvent(event)
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
    log.Printf("Failed to shutdown kafka consumer cleanly: %v", err)
  }  

  close(this.messages)

}