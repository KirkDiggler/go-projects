package dynamo

import (
	"context"
	"errors"
	"testing"

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
	return &Client{awsClient: &MockDynamoDB{}}
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
			AWSClient: &MockDynamoDB{},
		})

		assert.Nil(t, err)
		assert.NotNil(t, actual)
		assert.NotNil(t, actual.awsClient)
	})
}

func TestClient_PutItem(t *testing.T) {
	ctx := context.Background()
	testTableName := "test-table-name"

	testID := "uuid1-uuid-2uu-3-uuid4"
	testName := "my item"

	validItem := map[string]string{
		idFieldName:   testID,
		nameFieldName: testName,
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

		m := client.awsClient.(*MockDynamoDB)

		expectedErr := awserr.New("400", "dynamo down", errors.New("dynamo Down"))

		m.On("PutItemWithContext",
			ctx, mock.Anything, mock.Anything).Return(nil, expectedErr)

		actual, err := client.PutItem(ctx, testTableName,
			putitem.WithItem(&validItem))

		assert.Nil(t, actual)
		assert.NotNil(t, err)
		assert.Equal(t, expectedErr, err)
	})
	t.Run("it calls the aws client properly", func(t *testing.T) {
		client := setupFixture()

		m := client.awsClient.(*MockDynamoDB)

		expected := map[string]*dynamodb.AttributeValue{
			idFieldName:   {S: aws.String(validItem[idFieldName])},
			nameFieldName: {S: aws.String(validItem[nameFieldName])},
		}

		m.On("PutItemWithContext",
			ctx,
			&dynamodb.PutItemInput{
				Item:      expected,
				TableName: aws.String(testTableName),
			},
			mock.Anything).Return(&dynamodb.PutItemOutput{
			Attributes: expected,
		}, nil)

		actual, err := client.PutItem(ctx, testTableName,
			putitem.WithItem(&validItem))

		assert.Nil(t, err)
		assert.NotNil(t, actual)
		assert.Equal(t, expected, actual.Attributes)
	})
}
