package dynamo

import (
	"context"
	"errors"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"

	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"

	"github.com/KirkDiggler/go-projects/dynamo/inputs/putitem"

	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
)

const (
	minLengthTableName = 3

	requiredCfgMsg       = "cfg is required"
	requiredAWSClientMsg = "AWSClient is required"

	requiredTableNameMsg = "tableName is a required parameter"
	requiredItemMsg      = "item is required"
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

// PutItem
//
// PutItem can return aws Errors as well as simple errors.New()
func (c *Client) PutItem(ctx context.Context, tableName string, putOptions ...putitem.OptionFunc) (*putitem.Result, error) {
	if len(tableName) < minLengthTableName {
		return nil, errors.New(requiredTableNameMsg)
	}

	options := putitem.NewOptions(putOptions...)

	if options.Item == nil {
		return nil, errors.New(requiredItemMsg)
	}

	// TODO see if options.Item is a a map[string]*dynamodb.AttributeValue and skip this step
	inputItem, err := dynamodbattribute.MarshalMap(options.Item)
	if err != nil {
		return nil, err
	}

	dynamoInput := &dynamodb.PutItemInput{
		TableName: aws.String(tableName),
		Item:      inputItem,
	}

	result, err := c.awsClient.PutItemWithContext(ctx, dynamoInput)
	if err != nil {
		return nil, err
	}

	return &putitem.Result{
		Attributes: result.Attributes,
	}, nil
}
