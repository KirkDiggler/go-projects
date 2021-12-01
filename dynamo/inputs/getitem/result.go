package getitem

import (
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

type Result struct {
	Item             map[string]types.AttributeValue
	ConsumedCapacity *types.ConsumedCapacity
}
