package dynamo

import (
	"context"
	"errors"

	"github.com/KirkDiggler/go-projects/dynamo/inputs/query"

	"github.com/KirkDiggler/go-projects/dynamo/inputs/listtables"

	"github.com/KirkDiggler/go-projects/dynamo/inputs/getitem"

	"github.com/KirkDiggler/go-projects/dynamo/inputs/describetable"

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
	requiredAWSClientMsg = "the field  AWSClient is required"

	requiredTableNameMsg = "tableName is a required parameter"
	requiredItemMsg      = "the field  Item is required"

	requiredKeyMsg                 = "the field Key is required"
	requiredKeyConditionBuilderMsg = "the field KeyConditionBuilder is required"
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

// DescribeTable
func (c *Client) DescribeTable(ctx context.Context, tableName string) (*describetable.Result, error) {
	if len(tableName) < minLengthTableName {
		return nil, errors.New(requiredTableNameMsg)
	}

	dynamoInput := &dynamodb.DescribeTableInput{
		TableName: aws.String(tableName),
	}

	result, err := c.awsClient.DescribeTableWithContext(ctx, dynamoInput)
	if err != nil {
		return nil, err
	}

	return &describetable.Result{Table: result.Table}, nil
}

// GetItem
func (c *Client) GetItem(ctx context.Context, tableName string, getOptions ...getitem.OptionFunc) (*getitem.Result, error) {
	if len(tableName) < minLengthTableName {
		return nil, errors.New(requiredTableNameMsg)
	}

	options := getitem.NewOptions(getOptions...)

	if options.Key == nil {
		return nil, errors.New(requiredKeyMsg)
	}

	dynamoInput := &dynamodb.GetItemInput{
		Key:                    options.Key,
		AttributesToGet:        options.AttributesToGet,
		ConsistentRead:         options.ConsistentRead,
		ReturnConsumedCapacity: options.ReturnConsumedCapacity,
		TableName:              aws.String(tableName),
	}

	if options.ProjectionBuilder != nil {
		expr, err := expression.NewBuilder().WithProjection(*options.ProjectionBuilder).Build()
		if err != nil {
			return nil, err
		}

		dynamoInput.ProjectionExpression = expr.Projection()
		dynamoInput.ExpressionAttributeNames = expr.Names()
	}

	result, err := c.awsClient.GetItemWithContext(ctx, dynamoInput)
	if err != nil {
		return nil, err
	}

	return &getitem.Result{
		Item:             result.Item,
		ConsumedCapacity: result.ConsumedCapacity,
	}, nil
}

// ListTables
func (c *Client) ListTables(ctx context.Context, listTableOptions ...listtables.OptionFunc) (*listtables.Result, error) {
	options := listtables.NewOptions(listTableOptions...)

	dynamoInput := &dynamodb.ListTablesInput{
		ExclusiveStartTableName: options.ExclusiveStartTableName,
		Limit:                   options.Limit,
	}

	result, err := c.awsClient.ListTablesWithContext(ctx, dynamoInput)
	if err != nil {
		return nil, err
	}

	return &listtables.Result{
		LastEvaluatedTableName: result.LastEvaluatedTableName,
		TableNames:             result.TableNames,
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

// Query
func (c *Client) Query(ctx context.Context, tableName string, queryOptions ...query.OptionFunc) (*query.Result, error) {
	if len(tableName) < minLengthTableName {
		return nil, errors.New(requiredTableNameMsg)
	}

	options := query.NewOptions(queryOptions...)

	if options.KeyConditionBuilder == nil {
		return nil, errors.New(requiredKeyConditionBuilderMsg)
	}

	builder := expression.NewBuilder().WithKeyCondition(*options.KeyConditionBuilder)

	if options.ProjectionBuilder != nil {
		builder.WithProjection(*options.ProjectionBuilder)
	}

	if options.FilterConditionBuilder != nil {
		builder.WithFilter(*options.FilterConditionBuilder)
	}

	expr, err := builder.Build()
	if err != nil {
		return nil, err
	}

	dynamoInput := &dynamodb.QueryInput{
		ConsistentRead:            options.ConsistentRead,
		ExclusiveStartKey:         options.ExclusiveStartKey,
		ExpressionAttributeNames:  expr.Names(),
		ExpressionAttributeValues: expr.Values(),
		FilterExpression:          expr.Filter(),
		KeyConditionExpression:    expr.KeyCondition(),
		IndexName:                 options.IndexName,
		Limit:                     options.Limit,
		ProjectionExpression:      expr.Projection(),
		ReturnConsumedCapacity:    options.ReturnConsumedCapacity,
		ScanIndexForward:          options.ScanIndexForward,
		Select:                    options.Select,
		TableName:                 aws.String(tableName),
	}

	result, err := c.awsClient.QueryWithContext(ctx, dynamoInput)
	if err != nil {
		return nil, err
	}

	return &query.Result{
		ConsumedCapacity: result.ConsumedCapacity,
		Count:            result.Count,
		Items:            result.Items,
		LastEvaluatedKey: result.LastEvaluatedKey,
		ScannedCount:     result.ScannedCount,
	}, nil
}
