package hedwig

import "errors"

var (
	ErrNilHedwig          = errors.New("hedwig is nil, use `New` to create an instance")
	ErrAlreadyInitialized = errors.New("already initialized, disconnect before attempting again")
	ErrNoBindings         = errors.New("no bindings provided, need at least one")
	ErrNoConsumerSetting  = errors.New("consumer setting is nil")
)
