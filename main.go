package main

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/base64"
	"encoding/json"
	"io/ioutil"
	"strings"

	"github.com/aws/aws-lambda-go/lambda"
)

const (
	controlMessage = "CONTROL_MESSAGE"
	dataMessage    = "DATA_MESSAGE"

	resultStatusFailed  = "ProcessingFailed"
	resultStatusDropped = "Dropped"
	resultStatusOk      = "Ok"
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

func transformLogEvent(l LogEvent) string {
	return l.Message
}

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

func transformRecords(e Event) []ResultRecord {
	// Open the event
	resultRecords := []ResultRecord{}

	// For each record, transform the record.
	for _, r := range e.Records {
		gzippedData, err := base64.StdEncoding.DecodeString(r.Data)
		if err != nil {
			resultRecords = append(resultRecords, ResultRecord{
				RecordId: r.RecordId,
				Result:   resultStatusFailed,
			})
			continue
		}

		b := &bytes.Buffer{}
		if err = gunzip(b, gzippedData); err != nil {
			resultRecords = append(resultRecords, ResultRecord{
				RecordId: r.RecordId,
				Result:   resultStatusFailed,
			})
			continue
		}

		m := &Message{}
		if err = json.Unmarshal(b.Bytes(), m); err != nil {
			resultRecords = append(resultRecords, ResultRecord{
				RecordId: r.RecordId,
				Result:   resultStatusFailed,
			})
		}

		if m.MessageType == controlMessage {
			// Drop CONTROL_MESSAGEs. CONTROL_MESSAGEs are sent by CWL to check if
			// the subscription is reachable. They do not contain actual data.
			resultRecords = append(resultRecords, ResultRecord{
				RecordId: r.RecordId,
				Result:   resultStatusDropped,
			})

		} else if m.MessageType == dataMessage {
			// Transform DATA_MESSAGEs. Each DATA_MESSAGE has zero or more log
			// events. This logic transforms those log events.
			transformedLogEvents := []string{}
			for _, l := range m.LogEvents {
				t := transformLogEvent(l)
				if t != "" {
					transformedLogEvents = append(transformedLogEvents, t)
				}
			}

			var result ResultRecord
			if len(transformedLogEvents) > 0 {
				data := strings.Join(transformedLogEvents, "\n") + "\n"
				result = ResultRecord{
					RecordId: r.RecordId,
					Result:   resultStatusOk,
					Data:     base64.StdEncoding.EncodeToString([]byte(data)),
				}
			} else {
				// Drop the record if no log events resulted from the
				// transformations.
				result = ResultRecord{
					RecordId: r.RecordId,
					Result:   resultStatusDropped,
				}
			}

			resultRecords = append(resultRecords, result)
		} else {
			// Any message that is not a CONTROL_MESSAGE or a DATA_MESSAGE
			// should be considered a failure.
			resultRecords = append(resultRecords, ResultRecord{
				RecordId: r.RecordId,
				Result:   resultStatusFailed,
			})
		}
	}

	return resultRecords
}

func HandleRequest(ctx context.Context, e Event) (ResultResponse, error) {
	resultRecords := transformRecords(e)

	// Need to reingest

	// const isSas = Object.prototype.hasOwnProperty.call(event, 'sourceKinesisStreamArn');
	// const streamARN = isSas ? event.sourceKinesisStreamArn : event.deliveryStreamArn;
	// const region = streamARN.split(':')[3];
	// const streamName = streamARN.split('/')[1];
	// const result = { records: recs };
	// let recordsToReingest = [];
	// const putRecordBatches = [];
	// let totalRecordsToBeReingested = 0;
	// const inputDataByRecId = {};
	// event.records.forEach(r => inputDataByRecId[r.recordId] = createReingestionRecord(isSas, r));

	// let projectedSize = recs.filter(rec => rec.result === 'Ok')
	//                       .map(r => r.recordId.length + r.data.length)
	//                       .reduce((a, b) => a + b, 0);
	// // 6000000 instead of 6291456 to leave ample headroom for the stuff we didn't account for
	// for (let idx = 0; idx < event.records.length && projectedSize > 6000000; idx++) {

	return ResultResponse{
		Records: resultRecords,
	}, nil
}

func main() {
	lambda.Start(HandleRequest)
}
