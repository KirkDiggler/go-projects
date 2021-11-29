package query

import "github.com/aws/aws-sdk-go/service/dynamodb"

type Result struct {
	ConsumedCapacity *dynamodb.ConsumedCapacity
	Count            *int64
	Items            []map[string]*dynamodb.AttributeValue
	LastEvaluatedKey map[string]*dynamodb.AttributeValue
	ScannedCount     *int64
}
