package main

import (
	"fmt"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

func main() {

	p, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": "localhost"})
	if err != nil {
		panic(err)
	}

	defer p.Close()

	// Delivery report handler for produced messages
	go func() {
		for e := range p.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				if ev.TopicPartition.Error != nil {
					fmt.Printf("Delivery failed: %v\n", ev.TopicPartition)
				} else {
					fmt.Printf("Delivered message to %v\n", ev.TopicPartition)
				}
			}
		}
	}()

	// Produce messages to topic (asynchronously)
	topic := "smartcampusmaua"
	// `{"features":[{"properties":{"mag":7}},{"properties": {"mag": 12}}]}`
	// for _, word := range []string{"myMeasurement fieldKey=1"} {
	// for _, word := range []string{`{"features":[{"properties":{"mag":7}},{"properties": {"mag": 12}}]}`} {
	// word := `{"features":[{"properties":{"mag":7}},{"properties": {"mag": 12}}]}`
	// word := "myMeasurement fieldKey=1"
	word := "Measurement,tag1=tag1,tag2=tag2 fieldKey1=1,fieldKey2=2 1556813561098000000"
	// word := "APPDET,applicationID=1,nodeName=DET-32,devEUI=0004a30b00286d19,rxInfo_mac_0=7276ff000b032a92,rxInfo_name_0=Asp-klk-Agraria-02,rxInfo_mac_1=7276ff000b031df7,rxInfo_name_1=IMT-kerlink-blocoW2,rxInfo_mac_2=7276ff00080801db,rxInfo_name_2=IMT-kerlink-blocoH,txInfo_dataRate_modulation=LORA,txInfo_dataRate_bandwidth=125,txInfo_adr=true,txInfo_codeRate=4/5,fPort=100 rxInfo_rssi_0=-76,rxInfo_loRaSNR_0=9,rxInfo_latitude_0=-23,rxInfo_longitude_0=-46,rxInfo_longitude_0=-46,rxInfo_longitude_0=-46,rxInfo_altitude_0=746,rxInfo_rssi_1=-113,rxInfo_loRaSNR_1=-5,rxInfo_latitude_1=-23,rxInfo_longitude_1=-46,rxInfo_longitude_1=-46,rxInfo_longitude_1=-46,rxInfo_altitude_1=775,rxInfo_rssi_2=-114,rxInfo_loRaSNR_2=-5,rxInfo_latitude_2=-23,rxInfo_longitude_2=-46,rxInfo_longitude_2=-46,rxInfo_longitude_2=-46,rxInfo_altitude_2=759,txInfo_frequency=915800000,txInfo_dataRate_spreadFactor=7,data_humidity=308.4,fCnt=21 1701455025441"

	p.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Value:          []byte(word),
	}, nil)

	// Wait for message deliveries before shutting down
	p.Flush(15 * 1000)
}
