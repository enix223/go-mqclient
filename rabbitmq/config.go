package rabbitmq

// Config rabbitmq client config
type Config struct {
	RabbitMQ struct {
		// URL MQ endpoint url
		URL string

		// UseTLS whether use tls as underlying connection
		UseTLS bool

		// CACertPEM MQ CA Cert PEM content
		CACertPEM string

		// ClientCertBody MQ Client cert PEM content
		ClientCertPEM string

		// ClientKeyPEM MQ client key PEM content
		ClientKeyPEM string

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
}
