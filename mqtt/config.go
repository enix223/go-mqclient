package mqtt

// Config mqtt client config
type Config struct {
	MQTT struct {
		// ca cert path, need to provide when use_tls is on
		CACertPath string
		// client id
		ClientID string
		// username for the connection
		Username string
		// password for the connection
		Password string
		// host for the connection
		Host string
		// whether use tls connection or not
		// if yes, need to provide CACertPath as well
		UseTLS bool
		// whether try to reconnect if connection failed
		// or connection unexpectedly dropped
		Reconnect bool
		// interval in seconds between two reconnect retries
		ReconnectInternval int
		// how many times to try making connection
		// if equal 0, then reconnect forever
		ReconnectRetries int
		// the amount of time (in seconds) that the client
		// will wait after sending a PING request to the broker,
		PingTimeout int `default:"10"`
		// how long the client will wait when trying to open a connection
		// to an MQTT server before timeing out and erroring the attempt
		ConnectTimeout int `default:"30"`
		// the amount of time (in seconds) that the client
		// should wait before sending a PING request to the broker
		KeepAlive int `default:"30"`
		// the amount of time (in seconds) to wait for the connect flow
		// associated with the Token to complete
		ConnectTokenTimeout int `default:"10"`
		// the amount of time (in seconds) to wait for the publish flow
		// associated with the Token to complete
		PublishTokenTimeout int `default:"10"`
		// the amount of time (in seconds) to wait for the subscribe flow
		// associated with the Token to complete
		SubscribeTokenTimeout int `default:"10"`
		// the amount of time (in seconds) to wait for the unsubscribe flow
		// associated with the Token to complete
		UnSubscribeTokenTimeout int `default:"10"`
	}
}
