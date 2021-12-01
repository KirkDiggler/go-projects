package putitem

import (
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

type Result struct {
	Attributes            map[string]types.AttributeValue
	ConsumedCapacity      *types.ConsumedCapacity
	ItemCollectionMetrics *types.ItemCollectionMetrics
}
