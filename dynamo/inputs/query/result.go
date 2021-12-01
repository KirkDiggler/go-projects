package query

import (
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

type Result struct {
	ConsumedCapacity *types.ConsumedCapacity
	Count            int32
	Items            []map[string]types.AttributeValue
	LastEvaluatedKey map[string]types.AttributeValue
	ScannedCount     int32
}
