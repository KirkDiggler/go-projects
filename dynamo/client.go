package dynamo

import (
	"context"
	"errors"

	"github.com/KirkDiggler/go-projects/dynamo/inputs/deleteitem"

	"github.com/aws/aws-sdk-go/service/dynamodb/expression"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"

	"github.com/KirkDiggler/go-projects/dynamo/inputs/putitem"

	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
)

const (
	minLengthTableName = 3

	requiredCfgMsg       = "cfg is required"
	requiredAWSClientMsg = "AWSClient is required"

	requiredTableNameMsg = "tableName is a required parameter"
	requiredItemMsg      = "item is required"

	requiredKeyMsg = "key is required"
)

type Client struct {
	awsClient dynamodbiface.DynamoDBAPI
}

type ClientConfig struct {
	AWSClient dynamodbiface.DynamoDBAPI
}

func NewClient(cfg *ClientConfig) (*Client, error) {
	if cfg == nil {
		return nil, errors.New(requiredCfgMsg)
	}
	if cfg.AWSClient == nil {
		return nil, errors.New(requiredAWSClientMsg)
	}

	return &Client{
		awsClient: cfg.AWSClient,
	}, nil
}

// DeleteItem
func (c *Client) DeleteItem(ctx context.Context, tableName string, deleteOptions ...deleteitem.OptionFunc) (*deleteitem.Result, error) {
	if len(tableName) < minLengthTableName {
		return nil, errors.New(requiredTableNameMsg)
	}

	options := deleteitem.NewOptions(deleteOptions...)

	if options.Key == nil {
		return nil, errors.New(requiredKeyMsg)
	}

	dynamoInput := &dynamodb.DeleteItemInput{
		Key:                         options.Key,
		ReturnConsumedCapacity:      options.ReturnConsumedCapacity,
		ReturnItemCollectionMetrics: options.ReturnItemCollectionMetrics,
		ReturnValues:                options.ReturnValues,
		TableName:                   aws.String(tableName),
	}

	if options.ConditionalExpression != nil {
		expr, err := expression.NewBuilder().WithFilter(*options.ConditionalExpression).Build()
		if err != nil {
			return nil, err
		}

		dynamoInput.ConditionExpression = expr.Filter()
		dynamoInput.ExpressionAttributeNames = expr.Names()
		dynamoInput.ExpressionAttributeValues = expr.Values()
	}

	result, err := c.awsClient.DeleteItemWithContext(ctx, dynamoInput)
	if err != nil {
		return nil, err
	}

	return &deleteitem.Result{
		Attributes:            result.Attributes,
		ConsumedCapacity:      result.ConsumedCapacity,
		ItemCollectionMetrics: result.ItemCollectionMetrics,
	}, nil
}

// PutItem
func (c *Client) PutItem(ctx context.Context, tableName string, putOptions ...putitem.OptionFunc) (*putitem.Result, error) {
	if len(tableName) < minLengthTableName {
		return nil, errors.New(requiredTableNameMsg)
	}

	options := putitem.NewOptions(putOptions...)

	if options.Item == nil {
		return nil, errors.New(requiredItemMsg)
	}

	dynamoInput := &dynamodb.PutItemInput{
		TableName:                   aws.String(tableName),
		ReturnConsumedCapacity:      options.ReturnConsumedCapacity,
		ReturnItemCollectionMetrics: options.ReturnItemCollectionMetrics,
		ReturnValues:                options.ReturnValues,
		Item:                        options.Item,
	}

	if options.ConditionalExpression != nil {
		expr, err := expression.NewBuilder().WithFilter(*options.ConditionalExpression).Build()
		if err != nil {
			return nil, err
		}

		dynamoInput.ConditionExpression = expr.Filter()
		dynamoInput.ExpressionAttributeNames = expr.Names()
		dynamoInput.ExpressionAttributeValues = expr.Values()
	}

	result, err := c.awsClient.PutItemWithContext(ctx, dynamoInput)
	if err != nil {
		return nil, err
	}

	return &putitem.Result{
		Attributes:            result.Attributes,
		ConsumedCapacity:      result.ConsumedCapacity,
		ItemCollectionMetrics: result.ItemCollectionMetrics,
	}, nil
}
