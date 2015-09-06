package kafka

import ( 
        "github.com/Shopify/sarama"
        "log"
)


type Producer struct {
  syncProducer sarama.SyncProducer
}

// Constructor
func NewProducer(brokers []string) *Producer {
  config := sarama.NewConfig()
  config.Producer.RequiredAcks = sarama.WaitForAll 
  config.Producer.Retry.Max = 10    

  syncProducer, err := sarama.NewSyncProducer(brokers, config)
  if err != nil {
    log.Fatalln("Failed to start Sarama producer:", err)
    panic(err)
  }

  return &Producer{ 
    syncProducer : syncProducer,
  }
}

func (this *Producer) SendMessageToTopic( message []byte, topic string ) error {
  
  
  // send message
  _, _, err := this.syncProducer.SendMessage(&sarama.ProducerMessage {
      Topic: topic,
      Value: sarama.ByteEncoder(message),
    })

  if err != nil {
    return err
  }

  log.Println("message sent")
  return nil
}

func (this *Producer) Close() {
  if err := this.syncProducer.Close(); err != nil {
    log.Println("Failed to shut down kafka producer cleanly", err)
  }
}
