package hedwig

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/sirupsen/logrus"
)

const (
	PublishChannel   = "publish"
	SubscribeChannel = "subscribe"
)

const (
	MinDeliveryLimit uint = 1
	QosPrefetchCount      = "RMQ_PREFETCH_COUNT"
)

type Callback func(<-chan amqp.Delivery, *sync.WaitGroup)

type QueueType uint

const (
	QueueType_Classic QueueType = 0
	QueueType_Quorum  QueueType = 1
)

func DefaultSettings() *Settings {
	return &Settings{
		Exchange:          "hedwig",
		ExchangeType:      amqp.ExchangeTopic,
		ExchangeArgs:      nil,
		HeartBeatInterval: 5 * time.Second,
		SocketTimeout:     1 * time.Second,
		Host:              "localhost",
		Port:              5672,
		Consumer: &ConsumerSetting{
			Queues: make(map[string]*QueueSetting)},
	}
}

func DefaultQueueSetting(callback Callback, bindings ...string) *QueueSetting {
	return &QueueSetting{
		Bindings:  bindings,
		Durable:   true,
		Callback:  callback,
		QueueType: QueueType_Classic,
	}
}

func New(settings *Settings) *Hedwig {
	if settings == nil {
		settings = DefaultSettings()
	}
	return &Hedwig{Settings: settings, wg: &sync.WaitGroup{}, channels: make(map[string]*amqp.Channel), consumeTags: make(map[string]bool)}
}

type QueueSetting struct {
	Bindings   []string
	Callback   Callback
	Durable    bool
	AutoDelete bool
	Exclusive  bool
	NoAck      bool
	QueueType  QueueType
	// DeliveryLimit specifies the maximum number of times a message can be redelivered.
	// It is useful for preventing message poisoning by limiting how many times a message
	// can be retried before it is considered "poisoned" and either dead-lettered or discarded.
	// Note: This setting has no effect on Classic queues, which do not support delivery limits.
	// Caution: Once set, the DeliveryLimit cannot be updated during the lifetime of the queue.
	DeliveryLimit uint
}
type ConsumerSetting struct {
	tag    string
	Queues map[string]*QueueSetting
}

type Settings struct {
	Exchange          string
	ExchangeType      string
	ExchangeArgs      amqp.Table
	HeartBeatInterval time.Duration
	SocketTimeout     time.Duration
	Host              string
	Port              int
	Vhost             string
	Username          string
	Password          string
	Consumer          *ConsumerSetting
}

type Hedwig struct {
	sync.Mutex
	wg              *sync.WaitGroup
	Settings        *Settings
	Error           error
	conn            *amqp.Connection
	channels        map[string]*amqp.Channel
	consumeTags     map[string]bool
	closedChan      chan *amqp.Error
	customCloseChan chan *amqp.Error
}

func (h *Hedwig) AddQueue(qSetting *QueueSetting, qName string) error {
	h.Lock()
	defer h.Unlock()
	if h == nil {
		return ErrNilHedwig
	}
	if h.conn != nil {
		return ErrAlreadyInitialized
	}
	if len(qSetting.Bindings) == 0 {
		return ErrNoBindings
	}
	if h.Settings.Consumer == nil {
		return ErrNoConsumerSetting
	}
	if qName == "" {
		qName = fmt.Sprintf("AUTO-%d-%s", len(h.Settings.Consumer.Queues), strings.Join(qSetting.Bindings, "."))
		qSetting.Durable = false
		qSetting.Exclusive = true
	}
	h.Settings.Consumer.Queues[qName] = qSetting
	return nil
}

func (h *Hedwig) Publish(key string, body []byte) (err error) {
	return h.PublishWithHeaders(key, body, nil)
}

func (h *Hedwig) PublishWithDelay(key string, body []byte, delay time.Duration) (err error) {
	// from: https://www.rabbitmq.com/blog/2015/04/16/scheduling-messages-with-rabbitmq/
	// To delay a message a user must publish the message with the special header called x-delay which takes an integer
	// representing the number of milliseconds the message should be delayed by RabbitMQ.
	// It's worth noting that here delay means: delay message routing to queues or to other exchanges.
	headers := amqp.Table{DelayHeader: delay.Milliseconds()}
	return h.PublishWithHeaders(key, body, headers)
}

func (h *Hedwig) PublishWithHeaders(key string, body []byte, headers map[string]interface{}) (err error) {
	h.Lock()
	defer h.Unlock()

	c, err := h.getChannel(PublishChannel)
	if err != nil {
		return err
	}

	if err := c.Publish(h.Settings.Exchange, key, false, false, amqp.Publishing{
		Body:    body,
		Headers: headers,
	}); err != nil {
		// We already listen to closedChan [ref connect()] when connections are dropped.
		// In most cases github.com/streadway/amqp reports it.
		// We have observed some cases where this is not reported and we end with stale connections.
		// Only way to resolve this to restart the service to reconnect.

		// We manually check for error while publishing and if we get an error which says connection has been closed, we
		// notify on customCloseChan so that hedwig reconnects to RMQ
		if errors.Is(err, amqp.ErrClosed) {
			logrus.WithError(err).Error("Publish failed, reconnecting")
			h.customCloseChan <- amqp.ErrClosed
		}
		return err
	}
	return nil
}

func (h *Hedwig) Consume() error {
	if h == nil {
		return ErrNilHedwig
	}
	if h.Settings.Consumer == nil {
		return ErrNoConsumerSetting
	}
	defer h.Disconnect()

	err := h.setupListeners()

	if err != nil {
		return err
	}

	h.wg.Wait()

	return h.Error
}

func (h *Hedwig) setupListeners() (err error) {
	h.Lock()
	defer h.Unlock()
	c, err := h.getChannel(SubscribeChannel)
	if err != nil {
		return err
	}
	tag := 0
	for qName, qSetting := range h.Settings.Consumer.Queues {
		if len(qSetting.Bindings) == 0 {
			return ErrNoBindings
		}

		if strings.HasPrefix(qName, "AUTO-") {
			qName = ""
			qSetting.Durable = false
			qSetting.Exclusive = true
		}
		var queueArgs amqp.Table
		if qSetting.QueueType == QueueType_Quorum {
			queueArgs = amqp.Table{
				"x-queue-type": "quorum",
			}
			deliveryLimit := MinDeliveryLimit
			if qSetting.DeliveryLimit > deliveryLimit {
				deliveryLimit = qSetting.DeliveryLimit
			}
			queueArgs["x-delivery-limit"] = int32(deliveryLimit)
		}
		q, err := c.QueueDeclare(qName, qSetting.Durable, qSetting.AutoDelete, qSetting.Exclusive, false, queueArgs)
		if err != nil {
			return err
		}
		for _, binding := range qSetting.Bindings {
			err := c.QueueBind(q.Name, binding, h.Settings.Exchange, false, nil)
			if err != nil {
				return err
			}
		}

		consumeTag := q.Name + "-" + strconv.Itoa(tag)
		tag++
		h.consumeTags[consumeTag] = true
		delChan, err := c.Consume(
			q.Name,
			consumeTag,
			qSetting.NoAck,
			qSetting.Exclusive,
			false,
			false,
			nil,
		)
		if err != nil {
			return err
		}
		h.wg.Add(1)

		go qSetting.Callback(delChan, h.wg)
	}
	return nil

}

func (h *Hedwig) getChannel(name string) (ch *amqp.Channel, err error) {
	if v, ok := h.channels[name]; ok && h.channels[name] != nil {
		return v, nil
	}
	err = h.connect()
	if err != nil {
		return nil, err
	}

	channel, err := h.conn.Channel()
	if err != nil {
		return nil, err
	}
	err = channel.Qos(getIntEnv(QosPrefetchCount, false, 5000), 0, false)
	if err != nil {
		return nil, err
	}
	err = channel.ExchangeDeclare(
		h.Settings.Exchange, h.Settings.ExchangeType, true,
		false, false, false, h.Settings.ExchangeArgs)
	if err != nil {
		return nil, err
	}
	h.channels[name] = channel
	return h.channels[name], nil
}

func (h *Hedwig) Disconnect() error {
	h.Lock()
	defer h.Unlock()
	if h == nil {
		return ErrNilHedwig
	}
	if h.conn == nil {
		return nil
	}

	// Close all listening channels
	if len(h.consumeTags) > 0 {
		c, err := h.getChannel(SubscribeChannel)
		if err == nil {
			for tag := range h.consumeTags {
				go c.Cancel(tag, false)
			}
		}
		h.wg.Wait()
		h.consumeTags = make(map[string]bool)
	}

	err := h.conn.Close()

	if err != nil && err != amqp.ErrClosed {
		return err
	}
	return nil
}

func (h *Hedwig) connect() (err error) {
	if h == nil {
		return ErrNilHedwig
	}
	if h.conn != nil {
		return
	}
	h.Error = nil
	auth := []amqp.Authentication{
		&amqp.PlainAuth{
			Username: h.Settings.Username,
			Password: h.Settings.Password,
		},
	}

	h.conn, err = amqp.DialConfig(fmt.Sprintf("amqp://%s:%d", h.Settings.Host, h.Settings.Port), amqp.Config{
		SASL:      auth,
		Vhost:     h.Settings.Vhost,
		Heartbeat: h.Settings.HeartBeatInterval,
		Locale:    "en_US",
	})
	if err != nil {
		h.Error = err
		return
	}
	h.closedChan = make(chan *amqp.Error)
	h.customCloseChan = make(chan *amqp.Error)

	h.conn.NotifyClose(h.closedChan)
	go func() {
		closeErr, ok := <-h.closedChan
		if !ok {
			logrus.Warning("closedChan is closed")
			return
		}
		logrus.WithError(closeErr).Error("Recieved a connection closed event")
		h.Lock()
		defer h.Unlock()
		h.conn = nil
		h.channels = make(map[string]*amqp.Channel)
		h.consumeTags = make(map[string]bool)
		h.wg = &sync.WaitGroup{}
		if h.Error == nil {
			h.Error = closeErr
		}

	}()

	go func() {
		closeErr, ok := <-h.customCloseChan
		if !ok {
			logrus.Warning("customCloseChan is closed")
			return
		}
		logrus.WithError(closeErr).Error("Recieved a connection closed event")
		h.Lock()
		defer h.Unlock()
		h.conn = nil
		h.channels = make(map[string]*amqp.Channel)
		h.consumeTags = make(map[string]bool)
		h.wg = &sync.WaitGroup{}
		if h.Error == nil {
			h.Error = closeErr
		}

	}()

	return
}

func getIntEnv(key string, required bool, defaultValue int) int {
	val := os.Getenv(key)
	if val == "" {
		if required {
			logrus.Fatalf("%s not provided", key)
		}
		return defaultValue
	}
	intVal, err := strconv.Atoi(val)
	if err != nil {
		logrus.Fatalf("Invalid value provided for Key: %s", key)
	}
	return intVal
}
