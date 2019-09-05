package mqclient

import (
	"os"

	"github.com/jinzhu/configor"
	log "github.com/sirupsen/logrus"
)

const (
	// MQTypeRabbitMQ rabbitmq as backing MQ
	MQTypeRabbitMQ = "rabbitmq"
	// MQTypeMQTT MQTT as backing MQ
	MQTypeMQTT = "mqtt"
)

const (
	// EnvMQClientConfigPath environment variables for config path
	EnvMQClientConfigPath = "GO_MQCLIENT_CONFIG_PATH"
)

// CreateMQClient craete backing MQ client with given mq type
func CreateMQClient(typ string) Client {
	switch typ {
	case MQTypeRabbitMQ:
		var config RabbitMQConfig
		configor.Load(&config, os.Getenv(EnvMQClientConfigPath))
		return NewRabbitMQClient(config)
	case MQTypeMQTT:
		var config MQTTConfig
		configor.Load(&config, os.Getenv(EnvMQClientConfigPath))
		return NewMQTTClient(config)
	default:
		log.WithFields(
			log.Fields{"tag": "factory", "method": "CreateMQClient"},
		).Panicf("Does not support mq type: %v", typ)
		return nil
	}
}
