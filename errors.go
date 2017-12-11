package hedwig

import "errors"

var (
	ErrAlreadyInitialized = errors.New("already initialized, disconnect before attempting again")
	ErrNoBindings = errors.New("no bindings provided, need at least one")
	ErrNoConsumerSetting = errors.New("consumer setting is nil")
)