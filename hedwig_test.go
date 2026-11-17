package hedwig

import (
	"reflect"
	"testing"

	amqp "github.com/rabbitmq/amqp091-go"
)

func TestQueueArgs(t *testing.T) {
	tests := []struct {
		name    string
		setting QueueSetting
		want    amqp.Table
	}{
		{
			name:    "classic queue without args declares none",
			setting: QueueSetting{},
			want:    nil,
		},
		{
			name:    "quorum queue gets its type and the minimum delivery limit",
			setting: QueueSetting{QueueType: QueueType_Quorum},
			want:    amqp.Table{"x-queue-type": "quorum", "x-delivery-limit": int32(MinDeliveryLimit)},
		},
		{
			name:    "quorum queue keeps a higher delivery limit",
			setting: QueueSetting{QueueType: QueueType_Quorum, DeliveryLimit: 5},
			want:    amqp.Table{"x-queue-type": "quorum", "x-delivery-limit": int32(5)},
		},
		{
			name:    "classic queue passes its args through",
			setting: QueueSetting{Args: amqp.Table{"x-expires": int32(600000)}},
			want:    amqp.Table{"x-expires": int32(600000)},
		},
		{
			name: "queue type and delivery limit win over the same keys in args",
			setting: QueueSetting{
				QueueType:     QueueType_Quorum,
				DeliveryLimit: 3,
				Args:          amqp.Table{"x-expires": int32(600000), "x-queue-type": "classic", "x-delivery-limit": int32(9)},
			},
			want: amqp.Table{"x-expires": int32(600000), "x-queue-type": "quorum", "x-delivery-limit": int32(3)},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.setting.queueArgs(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("queueArgs() = %#v, want %#v", got, tt.want)
			}
		})
	}
}

func TestQueueArgsDoesNotChangeTheSetting(t *testing.T) {
	setting := QueueSetting{QueueType: QueueType_Quorum, Args: amqp.Table{"x-expires": int32(1000)}}
	setting.queueArgs()
	if want := (amqp.Table{"x-expires": int32(1000)}); !reflect.DeepEqual(setting.Args, want) {
		t.Errorf("Args = %#v, want %#v", setting.Args, want)
	}
}
