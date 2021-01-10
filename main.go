package main

import (
	"context"
	"fmt"
	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/influxdata/influxdb-client-go"
	"log"
	"os"
	"strconv"
	"strings"
	"time"
)

var (
	mqttAddr        = os.Getenv("MQTT_ADDR")
	inflxAddr       = os.Getenv("INFLX_ADDR")
	inflxCred       = os.Getenv("INFLX_CRED")
	inflxBucketName = os.Getenv("INFLX_BUCKET")
)

func main() {
	inflxdb := influxdb2.NewClient(inflxAddr, inflxCred)
	inflxWriteAPI := inflxdb.WriteAPIBlocking("", inflxBucketName)

	opts := mqtt.NewClientOptions()
	opts.AddBroker(fmt.Sprintf(mqttAddr))

	opts.SetClientID("equinox_measurement_service")

	opts.OnConnect = connectHandler
	opts.OnConnectionLost = connectLostHandler

	client := mqtt.NewClient(opts)
	if token := client.Connect(); token.Wait() && token.Error() != nil {
		log.Fatal(token.Error())
	}

	client.Subscribe("Equinox/#", byte(0), func(client mqtt.Client, message mqtt.Message) {
		topic := strings.Split(message.Topic(), "/")

		device := topic[1]
		measurement := topic[2]
		unit := topic[3]
		value, err := strconv.ParseFloat(string(message.Payload()), 32)
		if err != nil {
			log.Fatal(err)
		}

		p := influxdb2.NewPoint(measurement,
			map[string]string{"device": device, "unit": unit},
			map[string]interface{}{"value": value},
			time.Now())

		if err := inflxWriteAPI.WritePoint(context.Background(), p); err != nil {
			log.Fatal(err)
		}

		fmt.Printf("value: %f device: %s measurement: %s with unit: %s\n", value, device, measurement, unit)
	})

	fmt.Printf("Subscribed to equinox/#")

	select {}
}

func connectHandler(client mqtt.Client) {
	fmt.Println("Connected")
}

func connectLostHandler(client mqtt.Client, err error) {
	fmt.Printf("Connect lost: %v", err)
}
