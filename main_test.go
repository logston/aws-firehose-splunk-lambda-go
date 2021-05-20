package main

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestHandleRequest(t *testing.T) {
	ctx := context.Background()

	eventRecords := []EventRecord{
		{
			RecordId:                    "1",
			ApproximateArrivalTimestamp: 1621224132233,
			Data:                        "H4sICIkuo2AAA2RhdGEtbWVzc2FnZS5qc29uAOWSz07DMAzG73uKqOetchznH7dqlB0AgbTdEELZFqpKXTu1HQhNe3eSlm0vwAWRQ2T5+2znZ+U4YSzZ+a5zhV997X1yw5LbbJW9PebLZbbIk2k0NJ+1b6PEUZBU2lgYhaopFm1z2A9lrncPTXERln3r3S4qvi5nbr3x2/dr/cxV1WjtDutu05b7vmzqu7LqfduFopcgBXFMzM69Q/L13D//8HV/tR6HO0jlNs4UikuSINEqBMONISIVJmtLxmhSWqHRoLUBAdxKSSTDwZFr6NOXYS+920U4rpAjEhgDABfHz97iOGRXNBZ5YQQGa7SSJJAzDinnKU9RnGNMORomAIRi1kpiikkmQVzGEcVQiBBazbL5PH9esaf7ZHjAafqL3PyfcuMf4o6ff3KafANy+R18swMAAA==",
		},
	}

	e := Event{
		InvocationId:      "not-used",
		DeliveryStreamArn: "arn:aws:firehose:us-east-1:1234567890:deliverystream/DataLog",
		Region:            "us-east-1",
		Records:           eventRecords,
	}

	r, err := HandleRequest(ctx, e)
	require.NoError(t, err)

	rr := ResultResponse{
		Records: []ResultRecord{
			{
				RecordId:     "1",
				Result:       "Ok",
				Data:         "MiAxMjM0NTY3ODkwIGVuaS0wYWJjZWRmMDk4NzY1NDMyMSAxMC4xMS4xLjIzMSAxMC4xMS4yLjEyOCAzMDAzNiA5OTU0IDYgNSA1MDMgMTYyMTIyNDA0NCAxNjIzMzI0MDk3IEFDQ0VQVCBPSwoyIDEyMzQ1Njc4OTAgZW5pLTBhYmNlZGYwOTg3NjU0MzIxIDEwLjExLjEuMjMxIDEwLjExLjIuMTI4IDMwMDM2IDk5NTQgNiA1IDUwMyAxNjIxMjI0MDQ0IDE2MjMzMjQwOTcgQUNDRVBUIE9LCjIgMTIzNDU2Nzg5MCBlbmktMGFiY2VkZjA5ODc2NTQzMjEgMTAuMTEuMS4yMzEgMTAuMTEuMi4xMjggMzAwMzYgOTk1NCA2IDUgNTAzIDE2MjEyMjQwNDQgMTYyMzMyNDA5NyBBQ0NFUFQgT0sK",
				PartitionKey: "",
			},
		},
	}

	require.Equal(t, rr, r)
}

func TestEventRecordCreateReingestionRecord(t *testing.T) {
	for _, tc := range []struct {
		data         string
		expectedData string
		isSas        bool
	}{
		{
			data:         "dGVzdAo=",
			expectedData: "test\n",
			isSas:        true,
		},
		{
			data:         "dGVzdAo=",
			expectedData: "test\n",
			isSas:        false,
		},
	} {
		t.Run(fmt.Sprintf("isSas-%t", tc.isSas), func(t *testing.T) {
			er := EventRecord{
				Data: tc.data,
				KinesisMetadata: KinesisRecordMetadata{
					PartitionKey: "fakeKey",
				},
			}

			rr, err := er.createReingestionRecord(tc.isSas)
			require.NoError(t, err)

			require.Equal(t, tc.expectedData, rr.Data)
			if tc.isSas {
				require.Equal(t, "fakeKey", rr.PartitionKey)
			} else {
				require.Equal(t, "", rr.PartitionKey)
			}
		})
	}
}

func TestEvent(t *testing.T) {
	for _, tc := range []struct {
		isSas              bool
		expectedStreamARN  string
		expectedStreamName string
	}{
		{
			isSas:              true,
			expectedStreamARN:  "arn:aws:kinesis:us-east-1:1234567890:stream/DataLog1",
			expectedStreamName: "DataLog1",
		},
		{
			isSas:              false,
			expectedStreamARN:  "arn:aws:firehose:us-east-1:1234567890:deliverystream/DataLog2",
			expectedStreamName: "DataLog2",
		},
	} {
		t.Run(fmt.Sprintf("isSas-%t", tc.isSas), func(t *testing.T) {
			sourceKinesisStreamArn := ""
			if tc.isSas {
				sourceKinesisStreamArn = "arn:aws:kinesis:us-east-1:1234567890:stream/DataLog1"
			}

			e := Event{
				DeliveryStreamArn:      "arn:aws:firehose:us-east-1:1234567890:deliverystream/DataLog2",
				SourceKinesisStreamArn: sourceKinesisStreamArn,
			}

			require.Equal(t, tc.isSas, e.isSas())
			require.Equal(t, tc.expectedStreamARN, e.streamARN())
			require.Equal(t, tc.expectedStreamName, e.streamName())
		})
	}
}

func TestEventGetInputDataByRecId(t *testing.T) {
	for _, tc := range []struct {
		isSas        bool
		data         string
		expectedData string
	}{
		{
			isSas:        true,
			data:         "dGVzdDEK",
			expectedData: "test1\n",
		},
		{
			isSas:        false,
			data:         "dGVzdDIK",
			expectedData: "test2\n",
		},
	} {
		t.Run(fmt.Sprintf("isSas-%t", tc.isSas), func(t *testing.T) {
			e := Event{
				Records: []EventRecord{
					{
						RecordId: "12345",
						Data:     tc.data,
					},
				},
			}

			inputDataByRecId, err := e.getInputDataByRecId()
			require.NoError(t, err)

			rr := inputDataByRecId["12345"]

			require.Equal(t, tc.expectedData, rr.Data)
		})
	}
}

func TestResultRecordGetReingestionRecord(t *testing.T) {
}

func TestGunzip(t *testing.T) {
}

func TestTransformRecords(t *testing.T) {
}

func TestResultRecordListProjectedSize(t *testing.T) {
}

// Skipping these tests for now...
// func TestPutRecordsToKinesisStream(t *testing.T) {
// }

// func TestPutRecordsToFirehoseStream(t *testing.T) {
// }

// func TestPutBatches(t *testing.T) {
// }
