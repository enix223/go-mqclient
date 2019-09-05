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
package main

import (
	"flag"
	"time"

	mqclient "github.com/enix223/go-mqclient"

	log "github.com/sirupsen/logrus"
)

// specify the MQ client type, default to rabbitmq
var mqType = flag.String("type", mqclient.MQTypeRabbitMQ, "MQ client type")

func main() {
	flag.Parse()

	log.SetLevel(log.DebugLevel)

    // Factory to build a MQ client base on given type
    client := mqclient.CreateMQClient(*mqType)
    
    // Connect to the MQ server
	if err := client.Connect(); err != nil {
		log.Panic(err)
	}

	topic1 := "/abc/123"
	topic2 := "/abc/456"

	// subscribe
	if err := client.Subscribe(nil, onMessage, topic1, topic2); err != nil {
		log.Panic(err)
	}

	// publish routine
	go func() {
		i := 0
		topic := ""
		for {
			if i%2 == 0 {
				topic = topic1
			} else {
				topic = topic2
			}
			if err := client.PublishTopic(topic, []byte("123"), nil); err != nil {
				log.WithFields(log.Fields{"tag": "client", "err": err}).Error("publish failed")
			}
			time.Sleep(time.Second)
			i++
		}
    }()
    
    // You can try to shutdown the broker to test reconnect function now

    time.Sleep(20 * time.Second)
    // disconnect
	client.Disconnect()
}

func onMessage(payload *mqclient.Payload) {
	log.WithFields(log.Fields{"tag": "client"}).Infof("got message: %v", payload)
}
```
