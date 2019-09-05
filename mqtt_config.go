package mqclient

// MQTTConfig mqtt client config
type MQTTConfig struct {
	// ca cert path, need to provide when use_tls is on
	CACertPath string `yaml:"ca_cert_path"`
	// client id
	ClientID string `yaml:"client_id"`
	// username for the connection
	Username string `yaml:"username"`
	// password for the connection
	Password string `yaml:"password"`
	// host for the connection
	Host string `yaml:"host"`
	// whether use tls connection or not
	// if yes, need to provide CACertPath as well
	UseTLS bool `yaml:"use_tls"`
	// whether try to reconnect if connection failed
	// or connection unexpectedly dropped
	Reconnect bool `yaml:"reconnect"`
	// interval in seconds between two reconnect retries
	ReconnectInternval int `yaml:"reconnect_interval"`
	// how many times to try making connection
	// if equal 0, then reconnect forever
	ReconnectRetries int `yaml:"reconnect_retries"`
	// the amount of time (in seconds) that the client
	// will wait after sending a PING request to the broker,
	PingTimeout int `yaml:"ping_timeout"`
	// how long the client will wait when trying to open a connection
	// to an MQTT server before timeing out and erroring the attempt
	ConnectTimeout int `yaml:"connect_timeout"`
	// the amount of time (in seconds) that the client
	// should wait before sending a PING request to the broker
	KeepAlive int `yaml:"keep_alive"`
	// the amount of time (in seconds) to wait for the connect flow
	// associated with the Token to complete
	ConnectTokenTimeout int `yaml:"connect_token_timeout" default:"10"`
	// the amount of time (in seconds) to wait for the publish flow
	// associated with the Token to complete
	PublishTokenTimeout int `yaml:"publish_token_timeout" default:"10"`
	// the amount of time (in seconds) to wait for the subscribe flow
	// associated with the Token to complete
	SubscribeTokenTimeout int `yaml:"subscribe_token_timeout" default:"10"`
	// the amount of time (in seconds) to wait for the unsubscribe flow
	// associated with the Token to complete
	UnSubscribeTokenTimeout int `yaml:"unsubscribe_token_timeout" default:"10"`
}
