package main

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/base64"
	"encoding/json"
	"io/ioutil"

	"github.com/aws/aws-lambda-go/lambda"
)

type EventRecord struct {
	RecordId                    string `json:"recordId"`
	ApproximateArrivalTimestamp int    `json:"approximateArrivalTimestamp"`
	Data                        string `json:"data"`
}

type Event struct {
	InvocationId      string        `json:"invocationId"`
	DeliveryStreamArn string        `json:"deliveryStreamArn"`
	Region            string        `json:"region"`
	Records           []EventRecord `json:"records"`
}

type ResultRecord struct {
	RecordId string `json:"recordId"`
	Result   string `json:"result"`
	Data     string `json:"data"`
}

type ResultResponse struct {
	Records []ResultRecord `json:"records"`
}

type LogEvent struct {
	Id        string `json:"id"`
	Timestamp int    `json:"timestamp"`
	Message   string `json:"message"`
}

type Message struct {
	MessageType         string     `json:"messageType"`
	Owner               string     `json:"owner"`
	LogGroup            string     `json:"logGroup"`
	LogStream           string     `json:"logStream"`
	SubscriptionFilters []string   `json:"subscriptionFilters"`
	LogEvents           []LogEvent `json:"logEvents"`
}

func transformMessage() {}

func gunzip(b *bytes.Buffer, gzippedData []byte) error {
	gr, err := gzip.NewReader(bytes.NewBuffer(gzippedData))
	defer gr.Close()

	data, err := ioutil.ReadAll(gr)
	if err != nil {
		return err
	}

	_, err = b.Write(data)
	if err != nil {
		return err
	}

	return nil
}

func HandleRequest(ctx context.Context, e Event) (ResultResponse, error) {
	// Open the event
	resultRecords := []ResultRecord{}

	// For each record, transform the record.
	for _, r := range e.Records {
		gzippedData, err := base64.StdEncoding.DecodeString(r.Data)
		if err != nil {
			return ResultResponse{}, err
		}

		b := &bytes.Buffer{}
		if err = gunzip(b, gzippedData); err != nil {
			return ResultResponse{}, err
		}

		m := &Message{}
		if err = json.Unmarshal(b.Bytes(), m); err != nil {
			return ResultResponse{}, err
		}

		result := ResultRecord{
			RecordId: r.RecordId,
			Result:   "Ok",
			Data:     m.LogEvents[0].Message,
		}

		resultRecords = append(resultRecords, result)
	}

	response := ResultResponse{
		Records: resultRecords,
	}

	return response, nil
}

func main() {
	lambda.Start(HandleRequest)
}
