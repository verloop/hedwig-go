package hedwig

import (
	"fmt"
	"github.com/streadway/amqp"
	"strings"
	"sync"
	"time"
)

type Callback func(<-chan amqp.Delivery, *sync.WaitGroup)

func DefaultSettings() *Settings {
	return &Settings{
		Exchange:          "hedwig",
		ExchangeType:      "topic",
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
		Bindings: bindings,
		Durable:  true,
		Callback: callback,
	}
}

func New(settings *Settings) *Hedwig {
	if settings == nil {
		settings = DefaultSettings()
	}
	return &Hedwig{Settings: settings, wg: &sync.WaitGroup{}, channels: make(map[string]*amqp.Channel)}
}

type QueueSetting struct {
	Bindings   []string
	Callback   Callback
	Durable    bool
	AutoDelete bool
	Exclusive  bool
	NoAck      bool
}
type ConsumerSetting struct {
	tag    string
	Queues map[string]*QueueSetting
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
	Consumer          *ConsumerSetting
}

type Hedwig struct {
	sync.Mutex
	wg         *sync.WaitGroup
	Settings   *Settings
	Error      error
	conn       *amqp.Connection
	channels   map[string]*amqp.Channel
	closedChan chan *amqp.Error
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
	h.Lock()
	defer h.Unlock()

	c, err := h.getChannel("publish")
	if err != nil {
		return err
	}

	return c.Publish(h.Settings.Exchange, key, false, false, amqp.Publishing{
		Body: body,
	})
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
	c, err := h.getChannel("subscribe")
	if err != nil {
		return err
	}
	for qName, qSetting := range h.Settings.Consumer.Queues {
		if len(qSetting.Bindings) == 0 {
			return ErrNoBindings
		}

		if strings.HasPrefix(qName, "AUTO-") {
			qName = ""
			qSetting.Durable = false
			qSetting.Exclusive = true
		}
		q, err := c.QueueDeclare(qName, qSetting.Durable, qSetting.AutoDelete, qSetting.Exclusive, false, nil)
		if err != nil {

			return err
		}
		for _, binding := range qSetting.Bindings {
			err := c.QueueBind(q.Name, binding, h.Settings.Exchange, false, nil)
			if err != nil {
				return err
			}
		}

		delChan, err := c.Consume(
			q.Name,
			"",
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

	h.channels[name], err = h.conn.Channel()
	if err != nil {
		return nil, err
	}
	err = h.channels[name].ExchangeDeclare(
		h.Settings.Exchange, h.Settings.ExchangeType, true,
		false, false, false, nil)
	if err != nil {
		return nil, err
	}
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

	h.conn.NotifyClose(h.closedChan)
	go func() {
		closeErr := <-h.closedChan
		h.Lock()
		defer h.Unlock()
		h.conn = nil
		h.channels = make(map[string]*amqp.Channel)
		h.wg = &sync.WaitGroup{}
		if h.Error == nil {
			h.Error = closeErr
		}

	}()

	return
}
