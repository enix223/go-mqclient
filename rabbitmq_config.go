package mqclient

// RabbitMQConfig rabbitmq client config
type RabbitMQConfig struct {
	// URL MQ endpoint url
	URL string

	// UseTLS whether use tls as underlying connection
	UseTLS bool

	// CACertPath MQ CA cert path
	CACertPath string

	// ClientCertPath MQ Client cert path
	ClientCertPath string

	// ClientKeyPath MQ Client key path
	ClientKeyPath string

	// Exchange  exchange
	Exchange string

	// whether try to reconnect if connection failed
	// or connection unexpectedly dropped
	Reconnect bool `default:"false"`

	// interval in seconds between two reconnect retries
	ReconnectInternval int `default:"10"`

	// how many times to try making connection
	// if equal 0, then reconnect forever
	ReconnectRetries int `default:"0"`
}
