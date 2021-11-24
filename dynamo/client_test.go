package dynamo

import (
	"context"
	"errors"
	"testing"

	"github.com/KirkDiggler/go-projects/dynamo/inputs/deleteitem"

	"github.com/aws/aws-sdk-go/service/dynamodb/expression"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"

	"github.com/aws/aws-sdk-go/aws/awserr"

	"github.com/stretchr/testify/mock"

	"github.com/KirkDiggler/go-projects/dynamo/inputs/putitem"

	"github.com/stretchr/testify/assert"
)

const (
	idFieldName   = "id"
	nameFieldName = "name"
)

func setupFixture() *Client {
	return &Client{awsClient: &mockDynamoDB{}}
}

func TestNewClient(t *testing.T) {
	t.Run("it requires a config", func(t *testing.T) {
		actual, err := NewClient(nil)

		assert.Nil(t, actual)
		assert.NotNil(t, err)

		assert.Equal(t, errors.New(requiredCfgMsg), err)
	})
	t.Run("it requires an AWSClient", func(t *testing.T) {
		actual, err := NewClient(&ClientConfig{})

		assert.Nil(t, actual)
		assert.NotNil(t, err)

		assert.Equal(t, errors.New(requiredAWSClientMsg), err)
	})
	t.Run("it returns a client", func(t *testing.T) {
		actual, err := NewClient(&ClientConfig{
			AWSClient: &mockDynamoDB{},
		})

		assert.Nil(t, err)
		assert.NotNil(t, actual)
		assert.NotNil(t, actual.awsClient)
	})
}

func TestClient_DeleteItem(t *testing.T) {
	ctx := context.Background()
	testTableName := "test-table-name"

	testID := "uuid1-uuid2-uuid3-uuid4"
	testName := "my item"
	testReturnConsumedCapacity := aws.String("TOTAL")
	testReturnItemCollectionMetrics := aws.String("SIZE")
	testReturnValues := aws.String("ALL_OLD")

	validKey := map[string]*dynamodb.AttributeValue{
		idFieldName:   {S: aws.String(testID)},
		nameFieldName: {S: aws.String(testName)},
	}

	t.Run("it requires a table name to be 3 or more characters", func(t *testing.T) {
		client := setupFixture()

		actual, err := client.DeleteItem(ctx, "")

		assert.Nil(t, actual)
		assert.NotNil(t, err)
		assert.Equal(t, errors.New(requiredTableNameMsg), err)

		actual, err = client.DeleteItem(ctx, "to")

		assert.Nil(t, actual)
		assert.NotNil(t, err)
		assert.Equal(t, errors.New(requiredTableNameMsg), err)
	})
	t.Run("it requires a key to be set", func(t *testing.T) {
		client := setupFixture()

		actual, err := client.DeleteItem(ctx, testTableName)

		assert.Nil(t, actual)
		assert.NotNil(t, err)
		assert.Equal(t, errors.New(requiredKeyMsg), err)

		actual, err = client.DeleteItem(ctx, testTableName,
			deleteitem.WithKey(nil))

		assert.Nil(t, actual)
		assert.NotNil(t, err)
		assert.Equal(t, errors.New(requiredKeyMsg), err)
	})
	t.Run("it returns an error if the aws client returns an error", func(t *testing.T) {
		client := setupFixture()

		m := client.awsClient.(*mockDynamoDB)

		expectedErr := awserr.New("400", "dynamo down", errors.New("dynamo down"))

		m.On("DeleteItemWithContext",
			ctx, mock.Anything).Return(nil, expectedErr)

		actual, err := client.DeleteItem(ctx, testTableName,
			deleteitem.WithKey(validKey))

		assert.Nil(t, actual)
		assert.NotNil(t, err)
		assert.Equal(t, expectedErr, err)
	})
	t.Run("it calls the aws client properly", func(t *testing.T) {
		client := setupFixture()

		m := client.awsClient.(*mockDynamoDB)

		m.On("DeleteItemWithContext",
			ctx,
			&dynamodb.DeleteItemInput{
				Key:       validKey,
				TableName: aws.String(testTableName),
			}).Return(&dynamodb.DeleteItemOutput{
			Attributes: validKey,
		}, nil)

		actual, err := client.DeleteItem(ctx, testTableName,
			deleteitem.WithKey(validKey))

		assert.Nil(t, err)
		assert.NotNil(t, actual)
		assert.Equal(t, validKey, actual.Attributes)
	})
	t.Run("it sets all the parameters", func(t *testing.T) {
		client := setupFixture()

		m := client.awsClient.(*mockDynamoDB)
		filter := expression.Name(idFieldName).Equal(expression.Value(testID))
		filter = expression.And(expression.Name(nameFieldName).Equal(expression.Value(testName)), filter)
		expr, _ := expression.NewBuilder().WithFilter(filter).Build()

		expectedInput := &dynamodb.DeleteItemInput{
			Key:                         validKey,
			TableName:                   aws.String(testTableName),
			ReturnConsumedCapacity:      testReturnConsumedCapacity,
			ReturnItemCollectionMetrics: testReturnItemCollectionMetrics,
			ReturnValues:                testReturnValues,
			ConditionExpression:         expr.Filter(),
			ExpressionAttributeNames:    expr.Names(),
			ExpressionAttributeValues:   expr.Values(),
		}

		m.On("DeleteItemWithContext",
			ctx,
			expectedInput).Return(&dynamodb.DeleteItemOutput{
			Attributes: validKey,
		}, nil)

		actual, err := client.DeleteItem(ctx, testTableName,
			deleteitem.WithKey(validKey),
			deleteitem.WithReturnConsumedCapacity(testReturnConsumedCapacity),
			deleteitem.WithReturnItemCollectionMetrics(testReturnItemCollectionMetrics),
			deleteitem.WithReturnValues(testReturnValues),
			deleteitem.WithConditionalExpression(&filter))

		assert.Nil(t, err)
		assert.NotNil(t, actual)
		assert.Equal(t, validKey, actual.Attributes)
	})
}

func TestClient_DescribeTable(t *testing.T) {
	ctx := context.Background()
	testTableName := "test-table-name"

	t.Run("it requires a table name to be 3 or more characters", func(t *testing.T) {
		client := setupFixture()

		actual, err := client.DescribeTable(ctx, "")

		assert.Nil(t, actual)
		assert.NotNil(t, err)
		assert.Equal(t, errors.New(requiredTableNameMsg), err)

		actual, err = client.DescribeTable(ctx, "to")

		assert.Nil(t, actual)
		assert.NotNil(t, err)
		assert.Equal(t, errors.New(requiredTableNameMsg), err)
	})
	t.Run("it returns an error if the aws client returns an error", func(t *testing.T) {
		client := setupFixture()

		m := client.awsClient.(*mockDynamoDB)

		expectedErr := awserr.New("400", "dynamo down", errors.New("dynamo down"))

		m.On("DescribeTableWithContext",
			ctx, mock.Anything).Return(nil, expectedErr)

		actual, err := client.DescribeTable(ctx, testTableName)

		assert.Nil(t, actual)
		assert.NotNil(t, err)
		assert.Equal(t, expectedErr, err)
	})
	t.Run("it calls the aws client properly", func(t *testing.T) {
		client := setupFixture()

		returnedTable := &dynamodb.TableDescription{
			TableName: aws.String(testTableName),
		}
		m := client.awsClient.(*mockDynamoDB)

		m.On("DescribeTableWithContext",
			ctx,
			&dynamodb.DescribeTableInput{
				TableName: aws.String(testTableName),
			}).Return(&dynamodb.DescribeTableOutput{
			Table: returnedTable,
		}, nil)

		actual, err := client.DescribeTable(ctx, testTableName)

		assert.Nil(t, err)
		assert.NotNil(t, actual)
		assert.Equal(t, returnedTable, actual.Table)

		m.AssertExpectations(t)
	})
}

func TestClient_PutItem(t *testing.T) {
	ctx := context.Background()
	testTableName := "test-table-name"

	testID := "uuid1-uuid-2uu-3-uuid4"
	testName := "my item"
	testReturnConsumedCapacity := aws.String("TOTAL")
	testReturnItemCollectionMetrics := aws.String("SIZE")
	testReturnValues := aws.String("ALL_OLD")

	validItem := map[string]*dynamodb.AttributeValue{
		idFieldName:   {S: aws.String(testID)},
		nameFieldName: {S: aws.String(testName)},
	}

	t.Run("it requires a table name to be 3 or more characters", func(t *testing.T) {
		client := setupFixture()

		actual, err := client.PutItem(ctx, "")

		assert.Nil(t, actual)
		assert.NotNil(t, err)
		assert.Equal(t, errors.New(requiredTableNameMsg), err)

		actual, err = client.PutItem(ctx, "to")

		assert.Nil(t, actual)
		assert.NotNil(t, err)
		assert.Equal(t, errors.New(requiredTableNameMsg), err)
	})
	t.Run("it requires an item to be set", func(t *testing.T) {
		client := setupFixture()

		actual, err := client.PutItem(ctx, testTableName)

		assert.Nil(t, actual)
		assert.NotNil(t, err)
		assert.Equal(t, errors.New(requiredItemMsg), err)

		actual, err = client.PutItem(ctx, testTableName,
			putitem.WithItem(nil))

		assert.Nil(t, actual)
		assert.NotNil(t, err)
		assert.Equal(t, errors.New(requiredItemMsg), err)
	})
	t.Run("it returns an error if the aws client returns an error", func(t *testing.T) {
		client := setupFixture()

		m := client.awsClient.(*mockDynamoDB)

		expectedErr := awserr.New("400", "dynamo down", errors.New("dynamo Down"))

		m.On("PutItemWithContext",
			ctx, mock.Anything).Return(nil, expectedErr)

		actual, err := client.PutItem(ctx, testTableName,
			putitem.WithItem(validItem))

		assert.Nil(t, actual)
		assert.NotNil(t, err)
		assert.Equal(t, expectedErr, err)
	})
	t.Run("it calls the aws client properly", func(t *testing.T) {
		client := setupFixture()

		m := client.awsClient.(*mockDynamoDB)

		m.On("PutItemWithContext",
			ctx,
			&dynamodb.PutItemInput{
				Item:      validItem,
				TableName: aws.String(testTableName),
			}).Return(&dynamodb.PutItemOutput{
			Attributes: validItem,
		}, nil)

		actual, err := client.PutItem(ctx, testTableName,
			putitem.WithItem(validItem))

		assert.Nil(t, err)
		assert.NotNil(t, actual)
		assert.Equal(t, validItem, actual.Attributes)
	})
	t.Run("it sets all the parameters", func(t *testing.T) {
		client := setupFixture()

		m := client.awsClient.(*mockDynamoDB)
		filter := expression.Name(idFieldName).Equal(expression.Value(testID))
		filter = expression.And(expression.Name(nameFieldName).Equal(expression.Value(testName)), filter)
		expr, _ := expression.NewBuilder().WithFilter(filter).Build()

		expectedInput := &dynamodb.PutItemInput{
			Item:                        validItem,
			TableName:                   aws.String(testTableName),
			ReturnConsumedCapacity:      testReturnConsumedCapacity,
			ReturnItemCollectionMetrics: testReturnItemCollectionMetrics,
			ReturnValues:                testReturnValues,
			ConditionExpression:         expr.Filter(),
			ExpressionAttributeNames:    expr.Names(),
			ExpressionAttributeValues:   expr.Values(),
		}

		m.On("PutItemWithContext",
			ctx,
			expectedInput).Return(&dynamodb.PutItemOutput{
			Attributes: validItem,
		}, nil)

		actual, err := client.PutItem(ctx, testTableName,
			putitem.WithItem(validItem),
			putitem.WithReturnConsumedCapacity(testReturnConsumedCapacity),
			putitem.WithReturnItemCollectionMetrics(testReturnItemCollectionMetrics),
			putitem.WithReturnValues(testReturnValues),
			putitem.WithConditionalExpression(&filter))

		assert.Nil(t, err)
		assert.NotNil(t, actual)
		assert.Equal(t, validItem, actual.Attributes)
	})
}
