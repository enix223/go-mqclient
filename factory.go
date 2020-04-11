package mqclient

const (
	// EnvMQClientConfigPath environment variables for config path
	EnvMQClientConfigPath = "GO_MQCLIENT_CONFIG_PATH"
)

// Factory MQClient factory
type Factory interface {
	// Create a MQClient
	CreateMQClient() Client
}
