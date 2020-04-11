package mqclient

import "errors"

// OnDisconnect disconnect callback
// if user call Disconnect, then error will be nil
type OnDisconnect func(error)

// OnMessage message income callback
type OnMessage func(message *Message)

// Client an interface for message queue client
type Client interface {
	// Connect to mq server, and return error if failed
	Connect() error
	// Disconnect from the mq server
	Disconnect()
	// PublishTopic publish topic to the mq server
	PublishTopic(topic string, payload []byte, options map[string]interface{}) error
	// Subscribe given topics
	Subscribe(options map[string]interface{}, onMessage OnMessage, topics ...string) error
	// UnSubscribe consumer
	UnSubscribe(options map[string]interface{}, topics ...string) error
	// SetOnDisconnect set disconnect callback
	SetOnDisconnect(OnDisconnect)
}

// Subscription message subscription
type Subscription struct {
	Options   map[string]interface{}
	OnMessage OnMessage
}

// Message MQ message
type Message struct {
	Topic string
	Body  []byte
}

var (
	// ErrNotConnected client not connect to the server
	ErrNotConnected = errors.New("no connection to the server")
	// ErrTopicMissing topic parameter missing
	ErrTopicMissing = errors.New("missing topic")
	// ErrAlreadyConnected client had connected to server
	ErrAlreadyConnected = errors.New("already connected to server")
	// ErrTimeout operation timeout
	ErrTimeout = errors.New("operation timeout")
)
