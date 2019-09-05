package mqclient

import (
	"crypto/tls"
	"crypto/x509"
	"io/ioutil"
	"sync"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	log "github.com/sirupsen/logrus"
)

const (
	// reconnect delay in seconds
	defaultReconnectDelay = 10
	// mqtt connect timeout in seconds
	mqttTimeout = time.Duration(10 * time.Second)
)

// MQTTClient mqtt client
type MQTTClient struct {
	config MQTTConfig

	connectionM   sync.RWMutex
	connection    mqtt.Client
	stopReconnect chan struct{}

	disconnectCb OnDisconnect

	subscriptions sync.Map
}

// NewMQTTClient create a new mqtt client
func NewMQTTClient(config MQTTConfig) Client {
	m := &MQTTClient{
		config: config,
	}

	if config.ReconnectInternval == 0 {
		// apply default reconnect interval
		m.config.ReconnectInternval = defaultReconnectDelay
	}

	return m
}

// Connect make connection to mqtt server
func (m *MQTTClient) Connect() error {
	m.stopReconnect = make(chan struct{})

	// clear subscriptions
	m.subscriptions.Range(func(k, v interface{}) bool {
		m.subscriptions.Delete(k)
		return true
	})

	return m.connect()
}

func (m *MQTTClient) connect() error {
	opts := mqtt.NewClientOptions()
	if m.config.UseTLS {
		// Create TLS connection to mqtt server
		cfg := new(tls.Config)
		cfg.RootCAs = x509.NewCertPool()

		if ca, err := ioutil.ReadFile(m.config.CACertPath); err == nil {
			cfg.RootCAs.AppendCertsFromPEM(ca)
		} else {
			log.WithFields(
				log.Fields{"tag": "mqtt_client", "method": "Connect"},
			).Errorf("Failed to load Server certificate: %s", err)
			return err
		}

		opts = opts.SetTLSConfig(cfg)
	}

	opts.SetPingTimeout(time.Duration(m.config.PingTimeout) * time.Second)
	opts.SetConnectTimeout(time.Duration(m.config.ConnectTimeout) * time.Second)
	opts.SetKeepAlive(time.Duration(m.config.KeepAlive) * time.Second)

	opts.AddBroker(m.config.Host)
	opts.SetClientID(m.config.ClientID)
	opts.SetUsername(m.config.Username)
	opts.SetPassword(m.config.Password)
	opts.SetAutoReconnect(false)
	// connection lost handler
	opts.SetConnectionLostHandler(m.onDisconnect)
	// message handler
	opts.SetDefaultPublishHandler(m.onMessage)

	log.WithFields(
		log.Fields{"tag": "mqtt_client", "method": "Connect"},
	).Debugf("Connecting to MQTT server...")

	//create and start a client using the above ClientOptions
	m.connectionM.Lock()
	m.connection = mqtt.NewClient(opts)
	m.connectionM.Unlock()

	m.connectionM.RLock()
	token := m.connection.Connect()
	m.connectionM.RUnlock()

	if !token.WaitTimeout(time.Duration(m.config.ConnectTimeout) * time.Second) {
		log.WithFields(
			log.Fields{"tag": "mqtt_client", "method": "connect"},
		).Errorf("Failed to connect MQTT server: %v", ErrTimeout)
		return ErrTimeout
	}

	if token.Error() != nil {
		log.WithFields(
			log.Fields{"tag": "mqtt_client", "method": "connect"},
		).Errorf("Failed to connect MQTT server: %v", token.Error())
		return token.Error()
	}

	log.WithFields(
		log.Fields{"tag": "mqtt_client", "method": "Connect"},
	).Info("MQTT connection ready.")

	return nil
}

// SetOnDisconnect set disconnect callback
func (m *MQTTClient) SetOnDisconnect(cb OnDisconnect) {
	m.disconnectCb = cb
}

// Disconnect shutdown the mqtt client
func (m *MQTTClient) Disconnect() {
	log.WithFields(
		log.Fields{"tag": "mqtt_client", "method": "Disconnect"},
	).Info("Disconnecting MQTT server...")

	m.subscriptions.Range(func(k, v interface{}) bool {
		topic := k.(string)
		sub := v.(*subscription)
		if err := m.UnSubscribe(sub.options, topic); err != nil {
			log.WithFields(
				log.Fields{"tag": "mqtt_client", "method": "Disconnect"},
			).Errorf("Faild to unsubscribe topics: %v", err)
		}
		// cleanup
		m.subscriptions.Delete(k)
		return true
	})

	m.connectionM.RLock()
	defer m.connectionM.RUnlock()
	m.connection.Disconnect(250)

	log.WithFields(
		log.Fields{"tag": "mqtt_client", "method": "Disconnect"},
	).Info("MQTT server Disconnected")
}

// PublishTopic publish topic to the mq server
func (m *MQTTClient) PublishTopic(topic string, payload []byte, options map[string]interface{}) error {
	m.connectionM.RLock()
	defer m.connectionM.RUnlock()
	if !m.connection.IsConnected() {
		return ErrNotConnected
	}

	qos := 0
	timeout := time.Duration(m.config.ConnectTimeout) * time.Second
	retained := false
	if options != nil {
		if v, ok := options["qos"]; ok {
			if vv, ok := v.(int); ok {
				qos = vv
			}
		}
		if v, ok := options["timeout"]; ok {
			if vv, ok := v.(int); ok {
				timeout = time.Duration(vv) * time.Second
			}
		}
		if v, ok := options["retained"]; ok {
			if vv, ok := v.(bool); ok {
				retained = vv
			}
		}
	}

	token := m.connection.Publish(topic, byte(qos), retained, payload)
	if !token.WaitTimeout(timeout) {
		log.WithFields(
			log.Fields{"tag": "mqtt_client", "method": "PublishTopic", "err": ErrTimeout, "topic": topic},
		).Errorf("Failed to publish topic")
		return ErrTimeout
	}

	if token.Error() != nil {
		log.WithFields(
			log.Fields{"tag": "mqtt_client", "method": "PublishTopic", "err": token.Error(), "topic": topic},
		).Errorf("Failed to publish topic")
		return token.Error()
	}

	return nil
}

// Subscribe to given topics
func (m *MQTTClient) Subscribe(options map[string]interface{}, onMessage OnMessage, topics ...string) error {
	m.connectionM.RLock()
	defer m.connectionM.RUnlock()
	if !m.connection.IsConnected() {
		return ErrNotConnected
	}

	if len(topics) == 0 {
		return ErrTopicMissing
	}

	sub := subscription{
		options:   options,
		onMessage: onMessage,
	}

	for _, topic := range topics {
		if err := m.subscribe(&sub, topic); err != nil {
			return err
		}
		m.subscriptions.Store(topic, &sub)
	}

	return nil
}

func (m *MQTTClient) subscribe(sub *subscription, topic string) error {
	qos := 0
	timeout := time.Duration(m.config.SubscribeTokenTimeout) * time.Second
	if sub.options != nil {
		if v, ok := sub.options["qos"]; ok {
			if vv, ok := v.(int); ok {
				qos = vv
			}
		}
		if v, ok := sub.options["timeout"]; ok {
			if vv, ok := v.(int); ok {
				timeout = time.Duration(vv) * time.Second
			}
		}
	}

	m.connectionM.RLock()
	defer m.connectionM.RUnlock()
	token := m.connection.Subscribe(topic, byte(qos), m.onMessage)
	if !token.WaitTimeout(timeout) {
		log.WithFields(
			log.Fields{"tag": "mqtt_client", "method": "subscribe", "topic": topic, "err": ErrTimeout},
		).Errorf("Failed to subscribe topic")
		return ErrTimeout
	}

	if token.Error() != nil {
		log.WithFields(
			log.Fields{"tag": "mqtt_client", "method": "subscribe", "topic": topic, "err": token.Error()},
		).Errorf("Failed to subscribe topic")
		return token.Error()
	}

	log.WithFields(
		log.Fields{"tag": "mqtt_client", "method": "subscribe"},
	).Infof("Topic %s subscribed", topic)
	return nil
}

// UnSubscribe given topics
func (m *MQTTClient) UnSubscribe(options map[string]interface{}, topics ...string) error {
	m.connectionM.RLock()
	defer m.connectionM.RUnlock()
	if !m.connection.IsConnected() {
		return ErrNotConnected
	}

	var err error
	timeout := time.Duration(m.config.UnSubscribeTokenTimeout) * time.Second
	token := m.connection.Unsubscribe(topics...)
	if !token.WaitTimeout(timeout) {
		log.WithFields(
			log.Fields{"tag": "mqtt_client", "method": "Consume", "topics": topics, "err": ErrTimeout},
		).Errorf("unscribe topics failed")
		err = ErrTimeout
	}

	if token.Error() != nil {
		log.WithFields(
			log.Fields{"tag": "mqtt_client", "method": "Consume", "topics": topics, "err": token.Error()},
		).Errorf("unscribe topics failed")
		err = token.Error()
	}

	if err == nil {
		log.WithFields(
			log.Fields{"tag": "mqtt_client", "method": "Consume", "topics": topics},
		).Infof("topics unsubscribed")
	}

	// clean up
	for _, topic := range topics {
		if _, ok := m.subscriptions.Load(topic); ok {
			m.subscriptions.Delete(topic)
		}
	}

	return err
}

func (m *MQTTClient) onDisconnect(c mqtt.Client, err error) {
	if err != nil {
		// disconnect unexpectly
		log.WithFields(
			log.Fields{"tag": "mqtt_client", "method": "onDisconnect"},
		).Errorf("MQTT Connection dropped: %v", err)

		if m.config.Reconnect {
			// need to reconnect
			err = m.reconnect()
		}
	} else {
		log.WithFields(
			log.Fields{"tag": "mqtt_client", "method": "onDisconnect"},
		).Infof("MQTT disconnected")
	}

	if m.disconnectCb != nil {
		// emit disconnect event
		m.disconnectCb(err)
	}
}

func (m *MQTTClient) reconnect() error {
	interval := time.Duration(m.config.ReconnectInternval) * time.Second
	t := time.NewTicker(interval)
	retries := 0

	var err error
	for {
		log.WithFields(
			log.Fields{"tag": "mqtt_client", "method": "onDisconnect"},
		).Infof("Will reconnect in %d seconds...", m.config.ReconnectInternval)

		select {
		case <-t.C:
			// try reconnect
			if err = m.connect(); err == nil {
				// reconnect success, stop ticker
				t.Stop()

				// connect success, subscribe topics again
				m.resubscribe()

				return nil
			}
		case <-m.stopReconnect:
			// stop reconnect handler
			t.Stop()
			return nil
		}

		retries++
		if m.config.ReconnectRetries > 0 && retries >= m.config.ReconnectRetries {
			// stop ticker
			t.Stop()

			log.WithFields(
				log.Fields{"tag": "mqtt_client", "method": "onDisconnect"},
			).Info("Stop reconnect")

			return err
		}
	}
}

func (m *MQTTClient) resubscribe() {
	m.subscriptions.Range(func(k, v interface{}) bool {
		topic := k.(string)
		sub := v.(*subscription)
		if err := m.subscribe(sub, topic); err != nil {
			log.WithFields(
				log.Fields{"tag": "mqtt_client", "method": ""},
			).Errorf("subscribe topic %s failed: %v", topic, err)
		}
		return true
	})
}

func (m *MQTTClient) onMessage(c mqtt.Client, msg mqtt.Message) {
	log.WithFields(
		log.Fields{"tag": "mqtt_client", "method": "onMessage"},
	).Debugf("Got mqtt topic: %s, payload: %v", msg.Topic(), msg.Payload())

	if sub, ok := m.subscriptions.Load(msg.Topic()); ok && sub.(*subscription).onMessage != nil {
		sub.(*subscription).onMessage(&Payload{
			Topic: msg.Topic(),
			Body:  msg.Payload(),
		})
	} else {
		log.WithFields(
			log.Fields{"tag": "mqtt_client", "method": "onMessage"},
		).Warnf("Message callback is missing for topic: %v", msg.Topic())
	}
}