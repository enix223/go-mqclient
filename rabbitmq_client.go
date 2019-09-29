package mqclient

import (
	"crypto/tls"
	"crypto/x509"
	"io/ioutil"
	"runtime/debug"
	"sync"
	"time"

	"github.com/streadway/amqp"

	log "github.com/sirupsen/logrus"
)

// RBClient rabbitmq client
type RBClient struct {
	config RabbitMQConfig

	connectionM   sync.RWMutex
	connection    *amqp.Connection
	channelClosed chan *amqp.Error
	stopReconnect chan struct{}

	channelM sync.RWMutex
	channel  *amqp.Channel

	exchange string

	disconnectCb  OnDisconnect
	subscriptions sync.Map // map[string]rbSubscription
}

type rbSubscription struct {
	subscription
	closeCh chan struct{}
}

// NewRabbitMQClient create a new MQ Client
func NewRabbitMQClient(config RabbitMQConfig) Client {
	r := new(RBClient)

	r.config = config
	r.exchange = config.RabbitMQ.Exchange
	if len(r.exchange) == 0 {
		log.WithFields(
			log.Fields{"tag": "rabbitmq_client", "method": "Connect"},
		).Panicf("exchange setting is missing")
	}

	if len(config.RabbitMQ.URL) == 0 {
		// use default url
		r.config.RabbitMQ.URL = "amqp://guest:guest@localhost:5671//"
		r.config.RabbitMQ.UseTLS = false
	}

	return r
}

// SetOnDisconnect set disconnect callback
func (r *RBClient) SetOnDisconnect(cb OnDisconnect) {
	r.disconnectCb = cb
}

// Connect connect to MQ
func (r *RBClient) Connect() error {
	if err := r.connect(); err != nil {
		return err
	}

	// clear subscriptions
	r.subscriptions.Range(func(k, v interface{}) bool {
		r.subscriptions.Delete(k)
		return true
	})

	// handel channel close event
	r.channelClosed = make(chan *amqp.Error)
	go func() {
		for {
			select {
			case e, ok := <-r.channelClosed:
				if ok {
					// MQ channel closed emitted
					r.onChannelClosed(e)
				} else {
					// channel closed, stop handling close event
					log.WithFields(
						log.Fields{"tag": "rabbitmq_client", "method": "Connect"},
					).Debugf("Stop handling channel close event")
					return
				}
			}
		}
	}()

	return nil
}

func (r *RBClient) connect() error {
	r.connectionM.RLock()
	if r.connection != nil && r.connection.IsClosed() {
		r.connectionM.RUnlock()
		return ErrAlreadyConnected
	}
	r.connectionM.RUnlock()

	// create connection
	if err := r.createConnection(); err != nil {
		return err
	}

	// create channel
	if err := r.createChannel(); err != nil {
		return err
	}

	log.WithFields(
		log.Fields{"tag": "rabbitmq_client", "method": "connect"},
	).Infof("Connected to RabbitMQ server")

	return nil
}

// onChannelClosed handel channel close event
func (r *RBClient) onChannelClosed(e *amqp.Error) {
	r.connectionM.Lock()
	r.connection = nil
	r.connectionM.Unlock()

	r.channelM.Lock()
	r.channel = nil
	r.channelM.Unlock()

	if e != nil {
		// unexpectly disconnected
		log.WithFields(
			log.Fields{"tag": "rabbitmq_client", "method": "onChannelClosed"},
		).Errorf("Channel closed, err: %v", e)

		if r.config.RabbitMQ.Reconnect {
			// need reconnect
			if err := r.reconnect(); err != nil && err != ErrAlreadyConnected {
				// reconnect success
				log.WithFields(
					log.Fields{"tag": "rabbitmq_client", "method": "onChannelClosed"},
				).Infof("Reconnect failed: %v", err)
			}
		}
	} else {
		// disconnect by user
		log.WithFields(
			log.Fields{"tag": "rabbitmq_client", "method": "onChannelClosed"},
		).Info("Channel closed")
	}

	// emit disconnect event
	if r.disconnectCb != nil {
		r.disconnectCb(e)
	}
}

// reconnect try reconnect to the server
// will return nil if retry ok
// return error if retry exceed the limit
func (r *RBClient) reconnect() error {
	// create retry ticker
	interval := time.Duration(r.config.RabbitMQ.ReconnectInternval) * time.Second
	t := time.NewTicker(interval)
	retries := 0

	// chan to stop reconnect
	r.stopReconnect = make(chan struct{})

	var err error
	for {
		log.WithFields(
			log.Fields{"tag": "rabbitmq_client", "method": "Connect"},
		).Infof("Will reconnect in %d seconds...", r.config.RabbitMQ.ReconnectInternval)

		select {
		case <-t.C:
			// time to reconnect
			if err = r.connect(); err == nil || err == ErrAlreadyConnected {
				// connect success
				t.Stop()
				log.WithFields(
					log.Fields{"tag": "rabbitmq_client", "method": "Connect"},
				).Debugf("reconnect success")

				// make subscriptions again
				r.subscriptions.Range(func(k, v interface{}) bool {
					topic := k.(string)
					sub := v.(*rbSubscription)
					if err := r.subscribe(sub, topic); err != nil {
						log.WithFields(
							log.Fields{"tag": "rabbitmq_client", "method": "reconnect", "topic": topic, "err": err},
						).Error("subscribe failed")
					}
					return true
				})
				return nil
			}
		case <-r.stopReconnect:
			// stop reconnect
			t.Stop()
			log.WithFields(
				log.Fields{"tag": "rabbitmq_client", "method": "Connect", "reason": "stop by user"},
			).Debugf("stop reconnect routine")
			return nil
		}

		retries++
		if r.config.RabbitMQ.ReconnectRetries > 0 && retries >= r.config.RabbitMQ.ReconnectRetries {
			// reach max retries
			t.Stop()

			log.WithFields(
				log.Fields{"tag": "rabbitmq_client", "method": "Connect", "reason": "reach max retries"},
			).Debugf("stop reconnect routine")

			return err
		}
	}
}

// Disconnect Disconnect from MQ
func (r *RBClient) Disconnect() {
	// clean up
	if r.stopReconnect != nil {
		close(r.stopReconnect)
	}

	// close channel
	r.channelM.Lock()
	{
		if r.channel != nil {
			r.channel.Close()
			r.channel = nil
		}
	}
	r.channelM.Unlock()

	// close connection
	r.connectionM.Lock()
	{
		if r.connection != nil {
			r.connection.Close()
			r.connection = nil
		}
	}
	r.connectionM.Unlock()

	close(r.channelClosed)

	log.WithFields(
		log.Fields{"tag": "rabbitmq_client", "method": "Disconnect"},
	).Info("Connection disconnect successfully")
}

// PublishTopic produce topic
func (r *RBClient) PublishTopic(topic string, payload []byte, options map[string]interface{}) error {
	r.connectionM.RLock()
	r.channelM.RLock()
	if r.connection == nil || r.channel == nil || r.connection.IsClosed() {
		r.connectionM.RUnlock()
		r.channelM.RUnlock()
		return ErrNotConnected
	}
	r.connectionM.RUnlock()
	r.channelM.RUnlock()

	mandatory := false
	immediate := false

	if options != nil {
		if v, ok := options["mandatory"]; ok {
			if vv, ok := v.(bool); ok {
				mandatory = vv
			}
		}
		if v, ok := options["immediate"]; ok {
			if vv, ok := v.(bool); ok {
				mandatory = vv
			}
		}
	}

	r.channelM.RLock()
	defer r.channelM.RUnlock()
	err := r.channel.Publish(
		r.exchange, // exchange
		topic,      // routing key
		mandatory,  // mandatory
		immediate,  // immediate
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        payload,
		},
	)

	if err != nil {
		log.WithFields(
			log.Fields{"tag": "rabbitmq_client", "method": "PublishTopic"},
		).Errorf("Failed to publish topic: %v", err)
		return err
	}

	log.WithFields(
		log.Fields{"tag": "rabbitmq_client", "method": "PublishTopic"},
	).Debugf("Topic %s produced.", topic)
	return nil
}

// Subscribe subscribe topic
//
// Return a channel for consumer to read incoming data when topic is published by producer
func (r *RBClient) Subscribe(options map[string]interface{}, onMessage OnMessage, topics ...string) error {
	r.connectionM.RLock()
	r.channelM.RLock()
	if r.connection == nil || r.channel == nil || r.connection.IsClosed() {
		r.connectionM.RUnlock()
		r.channelM.RUnlock()
		return ErrNotConnected
	}
	r.connectionM.RUnlock()
	r.channelM.RUnlock()

	exchange := r.exchange
	queueName := ""
	consumer := ""
	durable := false
	delete := true
	exclusive := true
	noWait := false
	autoAck := true
	noLocal := false

	if options != nil {
		if v, ok := options["queueName"]; ok {
			if vv, ok := v.(string); ok {
				queueName = vv
			}
		}
		if v, ok := options["consumer"]; ok {
			if vv, ok := v.(string); ok {
				consumer = vv
			}
		}
		if v, ok := options["durable"]; ok {
			if vv, ok := v.(bool); ok {
				durable = vv
			}
		}
		if v, ok := options["delete"]; ok {
			if vv, ok := v.(bool); ok {
				delete = vv
			}
		}
		if v, ok := options["exclusive"]; ok {
			if vv, ok := v.(bool); ok {
				exclusive = vv
			}
		}
		if v, ok := options["noWait"]; ok {
			if vv, ok := v.(bool); ok {
				noWait = vv
			}
		}
		if v, ok := options["autoAck"]; ok {
			if vv, ok := v.(bool); ok {
				autoAck = vv
			}
		}
		if v, ok := options["noLocal"]; ok {
			if vv, ok := v.(bool); ok {
				noLocal = vv
			}
		}
	}

	opts := map[string]interface{}{
		"exchange":  exchange,
		"queueName": queueName,
		"consumer":  consumer,
		"durable":   durable,
		"delete":    delete,
		"exclusive": exclusive,
		"noWait":    noWait,
		"autoAck":   autoAck,
		"noLocal":   noLocal,
	}

	sub := rbSubscription{
		subscription: subscription{
			options:   opts,
			onMessage: onMessage,
		},
		closeCh: make(chan struct{}),
	}

	for _, topic := range topics {
		if err := r.subscribe(&sub, topic); err != nil {
			return err
		}
		r.subscriptions.Store(topic, &sub)
	}

	return nil
}

func (r *RBClient) subscribe(sub *rbSubscription, topic string) error {
	// create queue
	r.channelM.RLock()
	defer r.channelM.RUnlock()
	q, err := r.channel.QueueDeclare(
		sub.options["queueName"].(string), // queue name
		sub.options["durable"].(bool),     // durable
		sub.options["delete"].(bool),      // delete when usused
		sub.options["exclusive"].(bool),   // exclusive
		sub.options["noWait"].(bool),      // no-wait
		nil,                               // arguments
	)
	if err != nil {
		log.WithFields(
			log.Fields{"tag": "rabbitmq_client", "method": "subscribe"},
		).Errorf("Failed to create queue.")
		return err
	}

	// bind queue
	err = r.channel.QueueBind(
		q.Name,                           // queue name
		topic,                            // routing key
		sub.options["exchange"].(string), // exchange
		sub.options["noWait"].(bool),     // no-wait
		nil,
	)
	if err != nil {
		log.WithFields(
			log.Fields{"tag": "rabbitmq_client", "method": "subscribe"},
		).Errorf("Failed to bind queue.")
		return err
	}

	msgs, err := r.channel.Consume(
		q.Name,                           // queue
		sub.options["consumer"].(string), // consumer
		sub.options["autoAck"].(bool),    // auto-ack
		sub.options["exclusive"].(bool),  // exclusive
		sub.options["noLocal"].(bool),    // no-local
		sub.options["noWait"].(bool),     // no-wait
		nil,                              // args
	)
	if err != nil {
		log.WithFields(
			log.Fields{"tag": "rabbitmq_client", "method": "subscribe"},
		).Errorf("Failed to bind queue.")
		return err
	}

	go func(queueName, exchange string) {
		for {
			select {
			case d, ok := <-msgs:
				// publish message to consumers
				if ok && sub.onMessage != nil {
					handleMessage(sub, d.RoutingKey, d.Body)
				}
			case <-sub.closeCh:
				// unsubscribe, so clean up binding
				r.channelM.RLock()
				defer r.channelM.RUnlock()

				if r.channel != nil {
					r.channel.QueueUnbind(queueName, topic, exchange, nil)
					r.channel.QueueDelete(queueName, false, false, false)
					log.WithFields(
						log.Fields{"tag": "rabbitmq_client", "method": "subscribe"},
					).Infof("Topic unsubscribed.")
				}
				return
			}
		}
	}(q.Name, r.exchange)

	log.WithFields(
		log.Fields{"tag": "rabbitmq_client", "method": "subscribe"},
	).Infof("Topic %s subscribed.", topic)
	return nil
}

// handle incoming message, if onMessage crash, print the stack trace
func handleMessage(sub *rbSubscription, topic string, body []byte) {
	defer func() {
		if err := recover(); err != nil {
			log.WithFields(log.Fields{
				"tag":    "rabbitmq_client",
				"method": "handleMessage",
				"topic":  topic,
				"err":    err,
			}).Error("message handler crash")
			debug.PrintStack()
		}
	}()

	sub.onMessage(&Message{Topic: topic, Body: body})
}

// UnSubscribe consumer
func (r *RBClient) UnSubscribe(options map[string]interface{}, topics ...string) error {
	r.connectionM.RLock()
	r.channelM.RLock()
	if r.connection == nil || r.channel == nil || r.connection.IsClosed() {
		r.connectionM.RUnlock()
		r.channelM.RUnlock()
		return ErrNotConnected
	}
	r.connectionM.RUnlock()
	r.channelM.RUnlock()

	queueName := ""
	exchange := r.exchange
	ifUnused, ifEmpty, noWait := false, false, false
	if v, ok := options["exchange"]; ok {
		if vv, ok := v.(string); ok {
			exchange = vv
		}
	}
	if v, ok := options["queueName"]; ok {
		if vv, ok := v.(string); ok {
			queueName = vv
		}
	}
	if v, ok := options["ifUnused"]; ok {
		if vv, ok := v.(bool); ok {
			ifUnused = vv
		}
	}
	if v, ok := options["ifEmpty"]; ok {
		if vv, ok := v.(bool); ok {
			ifEmpty = vv
		}
	}
	if v, ok := options["noWait"]; ok {
		if vv, ok := v.(bool); ok {
			noWait = vv
		}
	}

	var e error
	for _, topic := range topics {
		if err := r.channel.QueueUnbind(queueName, topic, exchange, nil); err != nil {
			e = err
		}
		if _, err := r.channel.QueueDelete(queueName, ifUnused, ifEmpty, noWait); err != nil {
			e = err
		}
	}

	return e
}

// Connect connect to MQ
func (r *RBClient) createConnection() error {
	log.WithFields(
		log.Fields{"tag": "rabbitmq_client", "method": "createConnection"},
	).Debugf("Creating connection...")

	// see a note about Common Name (CN) at the top
	var err error
	urlStr := r.config.RabbitMQ.URL

	if r.config.RabbitMQ.UseTLS {
		sslCACertPath := r.config.RabbitMQ.CACertPath
		sslClientCertPath := r.config.RabbitMQ.ClientCertPath
		sslClientKeyPath := r.config.RabbitMQ.ClientKeyPath

		// Use TLS to connect to RabbitMQ
		cfg := new(tls.Config)
		cfg.RootCAs = x509.NewCertPool()

		if ca, err := ioutil.ReadFile(sslCACertPath); err == nil {
			cfg.RootCAs.AppendCertsFromPEM(ca)
		} else {
			log.WithFields(
				log.Fields{"tag": "rabbitmq_client", "method": "createConnection"},
			).Errorf("[MQ] Failed to load MQ Server certificate: %s", err)
			return err
		}

		if cert, err := tls.LoadX509KeyPair(sslClientCertPath, sslClientKeyPath); err == nil {
			cfg.Certificates = append(cfg.Certificates, cert)
		} else {
			log.WithFields(
				log.Fields{"tag": "rabbitmq_client", "method": "createConnection"},
			).Errorf("Failed to load client certificate/key pair: %s", err)
			return err
		}

		r.connectionM.Lock()
		defer r.connectionM.Unlock()
		r.connection, err = amqp.DialTLS(urlStr, cfg)
	} else {
		r.connectionM.Lock()
		defer r.connectionM.Unlock()
		r.connection, err = amqp.Dial(urlStr)
	}

	if err != nil {
		log.WithFields(
			log.Fields{"tag": "rabbitmq_client", "method": "createConnection"},
		).Errorf("Failed to connect to RabbitMQ: %s", err)
		return err
	}

	log.WithFields(
		log.Fields{"tag": "rabbitmq_client", "method": "createConnection"},
	).Debugf("Connection made.")
	return nil
}

// createChannel create amqp channel
func (r *RBClient) createChannel() error {
	var err error

	log.WithFields(
		log.Fields{"tag": "rabbitmq_client", "method": "createChannel"},
	).Debugf("Creating channel...")

	r.channelM.Lock()
	r.channel, err = r.connection.Channel()
	r.channelM.Unlock()

	if err != nil {
		log.WithFields(
			log.Fields{"tag": "rabbitmq_client", "method": "createChannel"},
		).Errorf("[MQ] failed to create channel: %v", err)
		return err
	}

	// create a bridge channel to listen channel close event
	// channel will be close when mq-channel closed
	channelClosed := make(chan *amqp.Error)
	go func() {
		select {
		case e := <-channelClosed:
			r.channelClosed <- e
		}
	}()

	// Listen to channel close notification
	if r.channel.NotifyClose(channelClosed); err != nil {
		r.Disconnect()
		log.WithFields(
			log.Fields{"tag": "rabbitmq_client", "method": "createChannel"},
		).Errorf("failed to subscribe channel close notification: %v", err)
		return err
	}

	log.WithFields(
		log.Fields{"tag": "rabbitmq_client", "method": "createChannel"},
	).Debugf("Channel made.")

	return nil
}
