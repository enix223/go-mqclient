package rabbitmq

import (
	"crypto/tls"
	"crypto/x509"
	"io/ioutil"
	"runtime/debug"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/streadway/amqp"

	mqclient "github.com/enix223/go-mqclient"
)

// Client rabbitmq client
type Client struct {
	topic         string
	url           string
	exchange      string
	logger        mqclient.Logger
	conn          *amqp.Connection
	channel       *amqp.Channel
	channelClosed chan *amqp.Error
	done          chan struct{}
	config        Config

	subscribeOptions map[string]interface{}
	subscribeTopics  []string

	disconnectCb mqclient.OnDisconnect
	messageCb    mqclient.OnMessage
}

// NewClient create a new MQ Client
func NewClient(config Config) mqclient.Client {
	logger := logrus.New()
	logger.SetFormatter(&logrus.TextFormatter{FullTimestamp: true})

	if len(config.RabbitMQ.LogLevel) != 0 {
		if l, err := logrus.ParseLevel(config.RabbitMQ.LogLevel); err == nil {
			logger.SetLevel(l)
		}
	} else {
		logger.Level = logrus.InfoLevel
	}

	a := &Client{
		exchange: config.RabbitMQ.Exchange,
		url:      config.RabbitMQ.URL,
		config:   config,
		done:     make(chan struct{}),
		logger:   logger,
	}

	return a
}

// SetOnDisconnect set disconnect callback
func (a *Client) SetOnDisconnect(cb mqclient.OnDisconnect) {
	a.disconnectCb = cb
}

// Connect connect to MQ
func (a *Client) Connect() error {
	a.connect()

	go func() {
		for {
			select {
			case <-a.done:
				// close the connection
				a.logger.Infof("Stopping client...")
				a.close()
				return
			case err := <-a.channelClosed:
				a.logger.Errorf("channel closed: %v", err)
				a.run()
			}
		}
	}()

	return nil
}

// Connect connect to MQ
func (a *Client) connect() {
	a.channelClosed = make(chan *amqp.Error)

	interval := time.Duration(a.config.RabbitMQ.ReconnectInternval) * time.Second
	mqclient.WaitUntil(a.done, func() bool {
		a.logger.Infof("Try to connect MQ: %s", a.url)
		if err := a.createConnection(); err != nil {
			a.logger.Errorf("failed to connect MQ: %v, will try after %d seconds...", err, a.config.RabbitMQ.ReconnectInternval)
			time.Sleep(interval)
			return false
		}
		return true
	})

	mqclient.WaitUntil(a.done, func() bool {
		a.logger.Debugf("Creating channel...")
		channel, err := a.conn.Channel()
		if err != nil {
			a.logger.Errorf("failed to create channel: %v, will try after %d seconds", err, a.config.RabbitMQ.ReconnectInternval)
			time.Sleep(interval)
			return false
		}
		a.channel = channel
		return true
	})

	a.channel.NotifyClose(a.channelClosed)
	a.logger.Infof("Connection created")
}

// PublishTopic publish topic to the mq server
func (a *Client) PublishTopic(topic string, payload []byte, options map[string]interface{}) error {
	if a.conn == nil || a.channel == nil || a.conn.IsClosed() {
		return mqclient.ErrNotConnected
	}

	exchange := a.exchange
	mandatory := false
	immediate := false

	if options != nil {
		if v, ok := options["exchange"]; ok {
			if vv, ok := v.(string); ok {
				exchange = vv
			}
		}
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

	err := a.channel.Publish(
		exchange,  // exchange
		topic,     // routing key
		mandatory, // mandatory
		immediate, // immediate
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        payload,
		},
	)
	if err != nil {
		a.logger.Errorf("Failed to publish topic: %v, err: %v", topic, err)
		return err
	}

	a.logger.Debugf("Message published. topic: %s, body: %s", topic, string(payload))

	return nil
}

// Subscribe given topics
func (a *Client) Subscribe(options map[string]interface{}, onMessage mqclient.OnMessage, topics ...string) error {
	a.messageCb = onMessage
	a.subscribeOptions = options
	a.subscribeTopics = topics

	go func() {
		a.subscribe(a.subscribeOptions, a.subscribeTopics...)
	}()

	return nil
}

// UnSubscribe consumer
func (a *Client) UnSubscribe(options map[string]interface{}, topics ...string) error {
	if a.conn == nil || a.channel == nil || a.conn.IsClosed() {
		return mqclient.ErrNotConnected
	}

	queueName := ""
	exchange := a.exchange
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
		if err := a.channel.QueueUnbind(queueName, topic, exchange, nil); err != nil {
			e = err
		}
		if _, err := a.channel.QueueDelete(queueName, ifUnused, ifEmpty, noWait); err != nil {
			e = err
		}
	}

	return e
}

// Disconnect from the mq server
func (a *Client) Disconnect() {
	close(a.done)
}

//
// Private
//

func (a *Client) subscribe(options map[string]interface{}, topics ...string) {
	if len(topics) == 0 {
		return
	}

	reconnectInterval := time.Duration(a.config.RabbitMQ.ReconnectInternval) * time.Second
	exchange := a.exchange
	queueName := ""
	consumer := ""
	durable := false
	delete := true
	exclusive := true
	noWait := false
	autoAck := true
	noLocal := false

	if options != nil {
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

	mqclient.WaitUntil(a.done, func() bool {
		a.logger.Debugf("Declaring queue %s...", queueName)
		_, err := a.channel.QueueDeclare(
			queueName, // queue name
			durable,   // durable
			delete,    // delete when usused
			exclusive, // exclusive
			noWait,    // no-wait
			nil,       // agrs
		)
		if err != nil {
			a.logger.Errorf("failed declare queue: %v, will try to reconnect after %d seconds...", err, a.config.RabbitMQ.ReconnectInternval)
			time.Sleep(reconnectInterval)
			return false
		}
		a.logger.Debugf("Queue declared")
		return true
	})

	for _, topic := range topics {
		mqclient.WaitUntil(a.done, func() bool {
			a.logger.Infof("Binding queue: %s, topic: %s, exchange: %s...", queueName, topic, exchange)
			err := a.channel.QueueBind(
				queueName, // queue name
				topic,     // topic
				exchange,  // exchange
				noWait,    // no-wait
				nil,
			)
			if err != nil {
				a.logger.Errorf("failed to bind queue: %v, will try to reconnect after %d seconds...", err, a.config.RabbitMQ.ReconnectInternval)
				time.Sleep(reconnectInterval)
				return false
			}
			a.logger.Debugf("Queue binded")
			return true
		})
	}

	var msgs <-chan amqp.Delivery
	mqclient.WaitUntil(a.done, func() bool {
		var err error
		msgs, err = a.channel.Consume(
			queueName, // queue name
			consumer,  // consumer
			autoAck,   // auto-ack
			exclusive, // exclusive
			noLocal,   // no-local
			noWait,    // no-wait
			nil,       // args
		)
		if err != nil {
			a.logger.Errorf("failed to consume msg: %v, will try after %d seconds...", err, reconnectInterval)
			time.Sleep(reconnectInterval)
			return false
		}
		return true
	})

	a.logger.Infof("message subscribed")

	for {
		select {
		case msg, ok := <-msgs:
			if ok {
				a.logger.Debugf("Got MQ message, topic: %s, body: %s", msg.RoutingKey, string(msg.Body))
				go a.handleMessage(msg.RoutingKey, msg.Body)
			}
		case <-a.channelClosed:
			a.logger.Errorf("message handler exit coz channel closed")
			return
		case <-a.done:
			for _, topic := range topics {
				a.channel.QueueUnbind(queueName, topic, a.exchange, nil)
			}
			a.channel.QueueDelete(queueName, false, false, false)
			a.logger.Infof("clear subscription")
			return
		}
	}
}

// handle incoming message, if onMessage crash, print the stack trace
func (a *Client) handleMessage(topic string, body []byte) {
	defer func() {
		if err := recover(); err != nil {
			a.logger.Errorf("message handler crash")
			debug.PrintStack()
		}
	}()

	if a.messageCb != nil {
		a.messageCb(&mqclient.Message{Topic: topic, Body: body})
	}
}

// Close close connection
func (a *Client) close() {
	a.logger.Infof("Closing rabbitmq client")

	if a.channel != nil {
		a.channel.Close()
	}
	if a.conn != nil {
		a.conn.Close()
	}
}

func (a *Client) run() {
	select {
	case <-a.done:
		return
	default:
		a.connect()
		a.subscribe(a.subscribeOptions, a.subscribeTopics...)
	}
}

// Connect connect to MQ
func (a *Client) createConnection() error {
	a.logger.Debugf("Creating connection...")

	// see a note about Common Name (CN) at the top
	var err error
	urlStr := a.config.RabbitMQ.URL

	if a.config.RabbitMQ.UseTLS {
		// Use TLS to connect to RabbitMQ
		cfg := new(tls.Config)
		cfg.RootCAs = x509.NewCertPool()

		if len(a.config.RabbitMQ.CACertPEM) > 0 {
			// Use PEM if set
			if !cfg.RootCAs.AppendCertsFromPEM([]byte(a.config.RabbitMQ.CACertPEM)) {
				a.logger.Errorf("[MQ] Failed to add CA certificate into root chain")
				return err
			}
		} else if len(a.config.RabbitMQ.CACertPath) > 0 {
			// Use ca cert file
			sslCACertPath := a.config.RabbitMQ.CACertPath
			if ca, err := ioutil.ReadFile(sslCACertPath); err == nil {
				cfg.RootCAs.AppendCertsFromPEM(ca)
			} else {
				a.logger.Errorf("[MQ] Failed to load MQ Server certificate: %s", err)
				return err
			}
		}

		if len(a.config.RabbitMQ.ClientKeyPEM) > 0 && len(a.config.RabbitMQ.ClientCertPEM) > 0 {
			// Use cert and key PEM
			if cert, err := tls.X509KeyPair([]byte(a.config.RabbitMQ.ClientCertPEM), []byte(a.config.RabbitMQ.ClientKeyPEM)); err == nil {
				cfg.Certificates = append(cfg.Certificates, cert)
			} else {
				a.logger.Errorf("Failed to load client certificate/key pair: %s", err)
				return err
			}
		} else if len(a.config.RabbitMQ.ClientCertPath) > 0 && len(a.config.RabbitMQ.ClientKeyPath) > 0 {
			// Use key & cert file
			sslClientCertPath := a.config.RabbitMQ.ClientCertPath
			sslClientKeyPath := a.config.RabbitMQ.ClientKeyPath
			if cert, err := tls.LoadX509KeyPair(sslClientCertPath, sslClientKeyPath); err == nil {
				cfg.Certificates = append(cfg.Certificates, cert)
			} else {
				a.logger.Errorf("Failed to load client certificate/key pair: %s", err)
				return err
			}
		}

		a.conn, err = amqp.DialTLS(urlStr, cfg)
	} else {
		a.conn, err = amqp.Dial(urlStr)
	}

	if err != nil {
		a.logger.Errorf("Failed to connect to RabbitMQ: %s", err)
		a.conn = nil
		return err
	}

	a.logger.Debugf("Connection made.")
	return nil
}
