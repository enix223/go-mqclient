# go-mqclient

A generic mq client library with uniform interfaces, which support rabbitmq, mqtt brokers

## Features:

1. Take care reconnect for you, and will also help you subscirbe the topics when reconnected
2. `yaml`/`json`/`toml`/`environment` based config to simplified library configuration. Thanks to [configor](https://github.com/jinzhu/configor)
3. Uniform interface which hides the detail of different brokers

## Install

```
go get -u github.com/enix223/go-mqclient
```

## Play with the example

For more detail please refer to `example/client.go`

1. Run with rabbitmq as broker

```
make example-rabbitmq
```

2. Run with paho eclipse mqtt sandbox server

```
make example-mqtt
```

## Usage

```go
// 1. Factory to build a MQ client base on given type
// rabbitmq as backing client
client := mqclient.CreateMQClient(mqclient.MQTypeRabbitMQ)
// or MQTT client
client := mqclient.CreateMQClient(mqclient.MQTypeMQTT)

// 2. Connect to the MQ server
err := client.Connect();

// 3. subscribe
err := client.Subscribe(nil, onMessage, topic1, topic2);

// 4. publish topic
err := client.PublishTopic(topic, []byte("123"), nil)

// 5. disconnect
client.Disconnect()

// 6. Message callback
func onMessage(payload *mqclient.Message) {
	log.Println(payload)
}
```
