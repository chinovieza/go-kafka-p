package main

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"strconv"
	"time"

	"github.com/Shopify/sarama"
)

func main() {

	//https://github.com/tcnksm-sample/sarama/blob/master/sync-producer/main.go

	kk := sarama.NewConfig()
	kk.Producer.RequiredAcks = sarama.WaitForAll
	kk.Producer.Retry.Max = 5
	kk.Producer.Return.Successes = true

	// brokers := strings.Split("10.198.107.58:9092,10.198.107.59:9092,10.198.107.60:9092", ",")
	brokers := []string{"10.198.107.58:9092", "10.198.107.59:9092", "10.198.107.60:9092"}
	producer, err := sarama.NewSyncProducer(brokers, kk)
	if err != nil {
		panic(err)
	}

	defer func() {
		if err := producer.Close(); err != nil {
			panic(err)
		}
	}()

	topic := "TEST_TOPIC"

	//-- mockup json package --//
	type dataJSON struct {
		CustomerID  string `json:"customer_id"`
		PackageCode string `json:"package_code"`
		CreateDate  string `json:"create_date"`
	}
	data := dataJSON{}

	for true {

		data.CustomerID = strconv.Itoa(rand.Intn(10000))
		data.PackageCode = "OPPA_RC_30D"
		data.CreateDate = time.Now().Format("2006-01-02 15:04:05")
		json, _ := json.Marshal(data)
		//-- end mockup json package --//

		msg := &sarama.ProducerMessage{
			Topic: topic,
			Value: sarama.StringEncoder(json),
		}

		partition, offset, err := producer.SendMessage(msg)
		if err != nil {
			panic(err)
		}

		fmt.Printf("Message is stored in topic(%s)/partition(%d)/offset(%d)\n(%s)\n\n", topic, partition, offset, json)

		time.Sleep(100 * time.Millisecond)

	}

}
