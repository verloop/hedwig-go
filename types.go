package hedwig

const (
	// For exchanges which use RMQ delay plugin, following type should be used as ExchangeType
	ExchangeTypeDelayed = "x-delayed-message"
	// Delayed Type Exchanges also need an extra arg with following key with the value set to actual ExchangeType
	// like `direct`, `topic` etc
	DelayedExchangeArgKey = "x-delayed-type"
	// Following header should be used when you are publishing to an Delay exchange. The header value will be
	// delay in milliseconds
	DelayHeader = "x-delay"
)
