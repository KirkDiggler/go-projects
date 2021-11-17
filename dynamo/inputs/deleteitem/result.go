package deleteitem

import "github.com/aws/aws-sdk-go/service/dynamodb"

type Result struct {
	Attributes            map[string]*dynamodb.AttributeValue
	ConsumedCapacity      *dynamodb.ConsumedCapacity
	ItemCollectionMetrics *dynamodb.ItemCollectionMetrics
}
