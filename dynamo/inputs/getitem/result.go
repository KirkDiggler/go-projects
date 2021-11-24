package getitem

import "github.com/aws/aws-sdk-go/service/dynamodb"

type Result struct {
	Item             map[string]*dynamodb.AttributeValue
	ConsumedCapacity *dynamodb.ConsumedCapacity
}
