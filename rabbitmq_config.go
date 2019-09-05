package mqclient

// RabbitMQConfig rabbitmq client config
type RabbitMQConfig struct {
	// URL MQ endpoint url
	URL string `yaml:"url"`

	// UseTLS whether use tls as underlying connection
	UseTLS bool `yaml:"use_tls"`

	// CACertPath MQ CA cert path
	CACertPath string `yaml:"ca_cert_path"`

	// ClientCertPath MQ Client cert path
	ClientCertPath string `yaml:"client_cert_path"`

	// ClientKeyPath MQ Client key path
	ClientKeyPath string `yaml:"client_key_path"`

	// Exchange  exchange
	Exchange string `yaml:"exchange"`

	// whether try to reconnect if connection failed
	// or connection unexpectedly dropped
	Reconnect bool `yaml:"reconnect"`

	// interval in seconds between two reconnect retries
	ReconnectInternval int `yaml:"reconnect_interval"`

	// how many times to try making connection
	// if equal 0, then reconnect forever
	ReconnectRetries int `yaml:"reconnect_retries"`
}
