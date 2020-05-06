package main

import (
	"github.com/ofpiyush/hedwig-go"
	"github.com/streadway/amqp"
)

// this is a brief example which explains how to setup a delay exchange queue
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
	return s
}
