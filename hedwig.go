package hedwig

import (
	"github.com/streadway/amqp"
	"time"
)

type Callback func()

func DefaultSettings() *Settings {
	return &Settings{
		Exchange:          "hedwig",
		ExchangeType:      "topic",
		HeartBeatInterval: 5 * time.Second,
		SocketTimeout:     1 * time.Second,
		Host:              "localhost",
		Port:              5672,
		ConsumerQueues:    make(map[string]*QueueSetting),
	}
}

func DefaultQueueSetting(bindings []string, callback Callback) *QueueSetting {
	return &QueueSetting{
		Bindings: bindings,
		Durable:  true,
		Callback: callback,
	}
}

func New(settings *Settings) *Hedwig {
	if settings == nil {
		settings = DefaultSettings()
	}
	return &Hedwig{Settings: settings}
}

type QueueSetting struct {
	Bindings   []string
	Callback   Callback
	Durable    bool
	AutoDelete bool
	Exclusive  bool
	NoAck      bool
}

type Settings struct {
	Exchange          string
	ExchangeType      string
	HeartBeatInterval time.Duration
	SocketTimeout     time.Duration
	Host              string
	Port              int
	Vhost             string
	Username          string
	Password          string
	ConsumerQueues    map[string]*QueueSetting
}

type Hedwig struct {
	Settings *Settings
	conn *amqp.Connection
	channel *amqp.Channel
}

func Publish() {

}
func Consume() {

}
