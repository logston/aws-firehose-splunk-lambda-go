package main

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"strings"

	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/firehose"
	"github.com/aws/aws-sdk-go/service/kinesis"
)

const (
	controlMessage = "CONTROL_MESSAGE"
	dataMessage    = "DATA_MESSAGE"

	resultStatusFailed  = "ProcessingFailed"
	resultStatusDropped = "Dropped"
	resultStatusOk      = "Ok"
)

type KinesisRecordMetadata struct {
	PartitionKey string `json:"partitionKey"`
}

type EventRecord struct {
	RecordId                    string                `json:"recordId"`
	ApproximateArrivalTimestamp int                   `json:"approximateArrivalTimestamp"`
	Data                        string                `json:"data"`
	KinesisMetadata             KinesisRecordMetadata `json:"kinesisRecordMetadata"`
}

func (er *EventRecord) createReingestionRecord(isSas bool) (ResultRecord, error) {
	data, err := base64.StdEncoding.DecodeString(er.Data)
	if err != nil {
		return ResultRecord{}, err
	}

	r := ResultRecord{
		Data: string(data),
	}

	if isSas {
		r.PartitionKey = er.KinesisMetadata.PartitionKey
	}

	return r, nil
}

type Event struct {
	InvocationId           string        `json:"invocationId"`
	DeliveryStreamArn      string        `json:"deliveryStreamArn"`
	SourceKinesisStreamArn string        `json:"sourceKinesisStreamArn"`
	Region                 string        `json:"region"`
	Records                []EventRecord `json:"records"`
}

func (e *Event) isSas() bool {
	return e.SourceKinesisStreamArn != ""
}

func (e *Event) streamARN() string {
	if e.isSas() {
		return e.SourceKinesisStreamArn
	} else {
		return e.DeliveryStreamArn
	}
}

func (e *Event) streamName() string {
	return strings.Split(e.streamARN(), "/")[1]
}

// getInputDataByRecId
func (e *Event) getInputDataByRecId() (map[string]ResultRecord, error) {
	inputDataByRecId := map[string]ResultRecord{}

	for _, r := range e.Records {
		rr, err := r.createReingestionRecord(e.isSas())
		if err != nil {
			return nil, err
		}

		inputDataByRecId[r.RecordId] = rr
	}

	return inputDataByRecId, nil
}

type ResultRecord struct {
	RecordId     string `json:"recordId"`
	Result       string `json:"result"`
	Data         string `json:"data"`
	PartitionKey string `json:"partitionKey"`
}

func (rr ResultRecord) getReingestionRecord(isSas bool) ResultRecord {
	r := ResultRecord{
		Data: rr.Data,
	}

	if isSas {
		r.PartitionKey = rr.PartitionKey
	}

	return r
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

func transformRecords(e Event) ResultRecordList {
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

type ResultRecordList []ResultRecord

// projectedSize returns the estimated size in bytes of the payload to
// be reingested.
func (rrl *ResultRecordList) projectedSize() int {
	total := 0
	for _, r := range *rrl {
		if r.Result == resultStatusOk {
			total += len(r.RecordId) + len(r.Data)
		}
	}
	return total
}

func putRecordsToFirehoseStream(
	svc *firehose.Firehose,
	streamName string,
	records []*firehose.Record,
	attempt int,
	maxAttempts int,
) error {
	var failed []*firehose.PutRecordBatchResponseEntry

	out, err := svc.PutRecordBatch(&firehose.PutRecordBatchInput{
		DeliveryStreamName: &streamName,
		Records:            records,
	})

	if err != nil {
		failed = out.RequestResponses
	} else if *out.FailedPutCount != 0 {
		codes := []string{}
		for _, r := range out.RequestResponses {
			r := r
			if *r.ErrorCode != "" {
				codes = append(codes, *r.ErrorCode)
				failed = append(failed, r)
			}
		}
		err = fmt.Errorf("Individual error codes: %s\n", strings.Join(codes, ","))
	}

	if len(failed) > 0 {
		if attempt+1 < maxAttempts {
			fmt.Printf("Some records failed while calling PutRecordBatch, retrying. %s\n", err)
			if err = putRecordsToFirehoseStream(svc, streamName, records, attempt+1, 20); err != nil {
				return err
			}
		} else {
			return fmt.Errorf("Could not put records after %d attempts. %s", maxAttempts, err)
		}
	}

	return nil
}

func putRecordsToKinesisStream(
	svc *kinesis.Kinesis,
	streamName string,
	records []*kinesis.PutRecordsRequestEntry,
	attempt int,
	maxAttempts int,
) error {
	var failed []*kinesis.PutRecordsResultEntry

	out, err := svc.PutRecords(&kinesis.PutRecordsInput{
		StreamName: &streamName,
		Records:    records,
	})
	if err != nil {
		failed = out.Records
	} else if *out.FailedRecordCount != 0 {
		codes := []string{}
		for _, r := range out.Records {
			r := r
			if *r.ErrorCode != "" {
				codes = append(codes, *r.ErrorCode)
				failed = append(failed, r)
			}
		}
		err = fmt.Errorf("Individual error codes: %s\n", strings.Join(codes, ","))
	}

	if len(failed) > 0 {
		if attempt+1 < maxAttempts {
			fmt.Printf("Some records failed while calling PutRecords, retrying. %s\n", err)
			if err = putRecordsToKinesisStream(svc, streamName, records, attempt+1, 20); err != nil {
				return err
			}
		} else {
			return fmt.Errorf("Could not put records after %d attempts. %s", maxAttempts, err)
		}
	}

	return nil
}

func putBatches(e Event, batches [][]ResultRecord, totalRecordsToBeReingested int) error {
	sess := session.Must(session.NewSession())

	recordsReingestedSoFar := 0
	for idx := 0; idx < len(batches); idx++ {
		batch := batches[idx]
		if e.isSas() {
			svc := kinesis.New(sess, aws.NewConfig().WithRegion(e.Region))
			svcRecords := []*kinesis.PutRecordsRequestEntry{}
			for _, r := range batch {
				svcRecords = append(svcRecords, &kinesis.PutRecordsRequestEntry{
					Data:         []byte(r.Data),
					PartitionKey: &r.PartitionKey,
				})
			}
			if err := putRecordsToKinesisStream(svc, e.streamName(), svcRecords, 0, 20); err != nil {
				fmt.Println("Failed to reingest records.")
				return err
			}
		} else {
			svc := firehose.New(sess, aws.NewConfig().WithRegion(e.Region))
			svcRecords := []*firehose.Record{}
			for _, r := range batch {
				svcRecords = append(svcRecords, &firehose.Record{Data: []byte(r.Data)})
			}
			if err := putRecordsToFirehoseStream(svc, e.streamName(), svcRecords, 0, 20); err != nil {
				fmt.Println("Failed to reingest records.")
				return err
			}
		}
		recordsReingestedSoFar += len(batch)
		fmt.Printf(
			"Reingested %d/%d records out of %d in to %s stream\n",
			recordsReingestedSoFar, totalRecordsToBeReingested, len(e.Records), e.streamName(),
		)
	}
	fmt.Printf(
		"Reingested all %d records out of %d in to %s stream\n",
		totalRecordsToBeReingested, len(e.Records), e.streamName(),
	)

	return nil
}

func HandleRequest(ctx context.Context, e Event) (ResultResponse, error) {
	resultRecords := transformRecords(e)

	ps := resultRecords.projectedSize()

	recordsToReingest := []ResultRecord{}
	putRecordBatches := [][]ResultRecord{}
	totalRecordsToBeReingested := 0

	inputDataByRecId, err := e.getInputDataByRecId()
	if err != nil {
		return ResultResponse{}, err
	}

	// 6000000 instead of 6291456 to leave ample headroom for the stuff we
	// didn't account for.
	for idx := 0; idx < len(e.Records) && ps > 6000000; idx++ {
		r := resultRecords[idx]
		if r.Result == resultStatusOk {
			totalRecordsToBeReingested++
			rtr := inputDataByRecId[r.RecordId].getReingestionRecord(e.isSas())
			recordsToReingest = append(recordsToReingest, rtr)

			r.Data = ""
			ps -= len(r.Data)

			resultRecords[idx].Result = resultStatusDropped

			if len(recordsToReingest) > 500 {
				putRecordBatches = append(putRecordBatches, recordsToReingest)
				recordsToReingest = []ResultRecord{}
			}
		}
	}

	if len(recordsToReingest) > 0 {
		// add the last batch
		putRecordBatches = append(putRecordBatches, recordsToReingest)
	}

	if len(putRecordBatches) > 0 {
		if err := putBatches(e, putRecordBatches, totalRecordsToBeReingested); err != nil {
			return ResultResponse{}, err
		}
	} else {
		fmt.Printf("No records needed to be reingested.")
	}

	return ResultResponse{
		Records: resultRecords,
	}, nil
}

func main() {
	lambda.Start(HandleRequest)
}
