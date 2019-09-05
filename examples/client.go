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

func onMessage(payload *mqclient.Message) {
	log.WithFields(log.Fields{"tag": "client"}).Infof("got message: %v", payload)
}
