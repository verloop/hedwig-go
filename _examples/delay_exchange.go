package main

import (
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	"github.com/ofpiyush/hedwig-go"
	"github.com/streadway/amqp"
)

// this is a short example which explains how to setup a delay exchange queue
// https://www.rabbitmq.com/blog/2015/04/16/scheduling-messages-with-rabbitmq/
func getDelayExchangeSettings() *hedwig.Settings {
	// get the preconfigured default exchange
	s := hedwig.DefaultSettings()
	// setup the exchange type to delay
	s.ExchangeType = hedwig.ExchangeTypeDelayed
	// set up the actual exchange behaviour like `direct`, `topic` etc
	s.ExchangeArgs = amqp.Table{
		hedwig.DelayedExchangeArgKey: amqp.ExchangeTopic,
	}
	// set up the exchange name and auth
	s.Exchange = "hedwig-delayed"
	s.Host = os.Getenv("RABBITMQ_HOST")
	s.Username = os.Getenv("RABBITMQ_USER")
	s.Password = os.Getenv("RABBITMQ_PASS")
	return s
}

func main() {
	settings := getDelayExchangeSettings()
	h := hedwig.New(settings)

	// set up the queue and bindings
	publishKey := "delayKey"
	binding := fmt.Sprintf("%s.*", publishKey)
	h.AddQueue(hedwig.DefaultQueueSetting(messageHandler, binding), "")

	// publisher
	go func() {
		for i := 0; i < 5; i++ {
			delay := time.Duration(i*10) * time.Second
			msg := fmt.Sprintf("I am: %d", int(delay.Seconds()))
			log.Println("\tPublishing: ", msg)
			err := h.PublishWithDelay(fmt.Sprintf("%s.0", publishKey), []byte(msg), delay)
			if err != nil {
				log.Println("publish failed:", err)
			}
		}
	}()

	err := h.Consume()
	log.Println(err)
	if err != nil {
		log.Fatalln("consume failed: ", err)
	}

}

func messageHandler(delChan <-chan amqp.Delivery, wg *sync.WaitGroup) {
	defer wg.Done()
	var err error
	for msg := range delChan {
		log.Println("\tBody: ", string(msg.Body), " headers: ", msg.Headers)
		err = msg.Ack(false)
		if err != nil {
			log.Println("ack error:", err)
		}
	}
}
