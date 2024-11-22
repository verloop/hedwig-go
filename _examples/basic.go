package main

import (
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/verloop/hedwig-go"
)

func main() {
	settings := hedwig.DefaultSettings()
	settings.Host = os.Getenv("RABBITMQ_HOST")
	settings.Username = os.Getenv("RABBITMQ_USER")
	settings.Password = os.Getenv("RABBITMQ_PASS")
	h := hedwig.New(settings)

	h.AddQueue(hedwig.DefaultQueueSetting(PingPonger(h, "alice"), "bob.*"), "")
	h.AddQueue(hedwig.DefaultQueueSetting(PingPonger(h, "bob"), "alice.*"), "")

	err := h.Consume()
	log.Println(err)
	if err != nil {
		log.Fatalln(err)
	}

}

func PingPonger(h *hedwig.Hedwig, name string) hedwig.Callback {
	displayName := fmt.Sprintf("[%s]\t", name)

	return func(delChan <-chan amqp.Delivery, wg *sync.WaitGroup) {
		defer wg.Done()
		var err error
		err = h.Publish(fmt.Sprintf("%s.0", name), []byte("Start message"))
		if err != nil {
			log.Println(name, err)
		}
		i := 1
		for msg := range delChan {
			log.Println(displayName, "got:", msg.RoutingKey, "\tBody:", string(msg.Body))
			time.Sleep(300 * time.Millisecond)
			err = h.Publish(fmt.Sprintf("%s.%d", name, i), []byte(fmt.Sprintf("Hello %d", i)))
			if err != nil {
				log.Println(displayName, "publish error:", err)
			}
			i++
			err = msg.Ack(false)
			if err != nil {
				log.Println(displayName, "ack error:", err)
			}
			if i > 100 {
				return
			}
		}
	}
}
