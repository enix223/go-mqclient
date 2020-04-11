package main

import (
	"flag"
	"os"
	"os/signal"
	"time"

	mqclient "github.com/enix223/go-mqclient"
	"github.com/enix223/go-mqclient/mqtt"
	"github.com/enix223/go-mqclient/rabbitmq"

	log "github.com/sirupsen/logrus"
)

const (
	mqTypeRabbitMQ = "rabbitmq"
	mqTypeMQTT     = "mqtt"
)

// specify the MQ client type, default to rabbitmq
var mqType = flag.String("type", mqTypeRabbitMQ, "MQ client type")

func main() {
	flag.Parse()

	log.SetLevel(log.DebugLevel)

	var client mqclient.Client

	switch *mqType {
	case mqTypeMQTT:
		log.Debug("using mqtt")
		client = mqtt.CreateMQClient()
	case mqTypeRabbitMQ:
		log.Debug("using rabbitmq")
		client = rabbitmq.CreateMQClient()
	default:
		panic("invalid type")
	}

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

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, os.Kill)
	<-c

	// disconnect
	client.Disconnect()
}

func onMessage(payload *mqclient.Message) {
	log.WithFields(log.Fields{"tag": "client"}).Infof("got message: %v", payload)
}
