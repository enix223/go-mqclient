.PHONY: example-rabbitmq
example-rabbitmq:
	cd examples && \
		CONFIGOR_DEBUG_MODE=1 \
		GO_MQCLIENT_CONFIG_PATH=./rabbitmq-config.yaml \
		go run client.go -type rabbitmq


.PHONY: example-mqtt
example-mqtt:
	cd examples && \
		CONFIGOR_DEBUG_MODE=1 \
		GO_MQCLIENT_CONFIG_PATH=./mqtt-config.yaml \
		go run client.go -type mqtt
