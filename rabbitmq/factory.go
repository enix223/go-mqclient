package rabbitmq

import (
	"os"

	"github.com/enix223/go-mqclient"
	"github.com/jinzhu/configor"
)

// CreateMQClient create rabbitmq client
func CreateMQClient() mqclient.Client {
	var config Config
	configor.Load(&config, os.Getenv(mqclient.EnvMQClientConfigPath))
	return NewClient(config)
}
