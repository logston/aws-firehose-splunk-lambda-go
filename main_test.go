package main

import (
	"context"
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

	require.Equal(t, nil, r)
}
