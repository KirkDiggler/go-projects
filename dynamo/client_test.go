package dynamo

import (
	"context"
	"errors"
	"testing"

	"github.com/KirkDiggler/go-projects/dynamo/inputs/deleteitem"
	"github.com/KirkDiggler/go-projects/dynamo/inputs/getitem"
	"github.com/KirkDiggler/go-projects/dynamo/inputs/listtables"
	"github.com/KirkDiggler/go-projects/dynamo/inputs/putitem"
	"github.com/KirkDiggler/go-projects/dynamo/inputs/query"
	"github.com/KirkDiggler/go-projects/dynamo/inputs/scan"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/mock"

	"github.com/stretchr/testify/assert"
)

const (
	idFieldName   = "id"
	nameFieldName = "name"
)

type testStruct struct {
	ID   string `dynamodbav:"id"`
	Name string `dynamodbav:"name"`
}

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
	testReturnConsumedCapacity := types.ReturnConsumedCapacityTotal
	testReturnItemCollectionMetrics := types.ReturnItemCollectionMetricsNone
	testReturnValues := types.ReturnValueAllOld

	validKey := map[string]types.AttributeValue{
		idFieldName:   &types.AttributeValueMemberS{Value: testID},
		nameFieldName: &types.AttributeValueMemberS{Value: testName},
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

		expectedErr := &types.InternalServerError{
			Message: aws.String("dynamo down"),
		}

		m.On("DeleteItem",
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

		m.On("DeleteItem",
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

		m.On("DeleteItem",
			ctx,
			expectedInput).Return(&dynamodb.DeleteItemOutput{
			Attributes: validKey,
		}, nil)

		actual, err := client.DeleteItem(ctx, testTableName,
			deleteitem.WithKey(validKey),
			deleteitem.WithReturnConsumedCapacity(testReturnConsumedCapacity),
			deleteitem.WithReturnItemCollectionMetrics(testReturnItemCollectionMetrics),
			deleteitem.WithReturnValue(testReturnValues),
			deleteitem.WithFilterConditionBuilder(&filter))

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

		expectedErr := &types.InternalServerError{
			Message: aws.String("dynamo down"),
		}

		m.On("DescribeTable",
			ctx, mock.Anything).Return(nil, expectedErr)

		actual, err := client.DescribeTable(ctx, testTableName)

		assert.Nil(t, actual)
		assert.NotNil(t, err)
		assert.Equal(t, expectedErr, err)
	})
	t.Run("it calls the aws client properly", func(t *testing.T) {
		client := setupFixture()

		returnedTable := &types.TableDescription{
			TableName: aws.String(testTableName),
		}
		m := client.awsClient.(*mockDynamoDB)

		m.On("DescribeTable",
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

func TestClient_GetItem(t *testing.T) {
	ctx := context.Background()

	testTableName := "test-table-name"
	testID := "uuid1-uuid2-uuid3-uuid4"
	testName := "my item"
	testReturnConsumedCapacity := types.ReturnConsumedCapacityTotal

	validKey := map[string]types.AttributeValue{
		idFieldName:   &types.AttributeValueMemberS{Value: testID},
		nameFieldName: &types.AttributeValueMemberS{Value: testName},
	}

	t.Run("it requires a table name to be 3 or more characters", func(t *testing.T) {
		client := setupFixture()

		actual, err := client.GetItem(ctx, "")

		assert.Nil(t, actual)
		assert.NotNil(t, err)
		assert.Equal(t, errors.New(requiredTableNameMsg), err)

		actual, err = client.GetItem(ctx, "to")

		assert.Nil(t, actual)
		assert.NotNil(t, err)
		assert.Equal(t, errors.New(requiredTableNameMsg), err)
	})
	t.Run("it requires a key to be set", func(t *testing.T) {
		client := setupFixture()

		actual, err := client.GetItem(ctx, testTableName)

		assert.Nil(t, actual)
		assert.NotNil(t, err)
		assert.Equal(t, errors.New(requiredKeyMsg), err)

		actual, err = client.GetItem(ctx, testTableName,
			getitem.WithKey(nil))

		assert.Nil(t, actual)
		assert.NotNil(t, err)
		assert.Equal(t, errors.New(requiredKeyMsg), err)
	})
	t.Run("it returns an error if the aws client returns an error", func(t *testing.T) {
		client := setupFixture()

		m := client.awsClient.(*mockDynamoDB)

		expectedErr := &types.InternalServerError{
			Message: aws.String("dynamo down"),
		}

		m.On("GetItem",
			ctx, mock.Anything).Return(nil, expectedErr)

		actual, err := client.GetItem(ctx, testTableName,
			getitem.WithKey(validKey))

		assert.Nil(t, actual)
		assert.NotNil(t, err)
		assert.Equal(t, expectedErr, err)
	})
	t.Run("it calls the aws client properly", func(t *testing.T) {
		client := setupFixture()

		m := client.awsClient.(*mockDynamoDB)

		m.On("GetItem",
			ctx,
			&dynamodb.GetItemInput{
				Key:       validKey,
				TableName: aws.String(testTableName),
			}).Return(&dynamodb.GetItemOutput{
			Item: validKey,
		}, nil)

		actual, err := client.GetItem(ctx, testTableName,
			getitem.WithKey(validKey))

		assert.Nil(t, err)
		assert.NotNil(t, actual)
		assert.Equal(t, validKey, actual.Item)
	})
	t.Run("it returns AsEntity", func(t *testing.T) {
		client := setupFixture()

		m := client.awsClient.(*mockDynamoDB)

		m.On("GetItem",
			ctx,
			&dynamodb.GetItemInput{
				Key:       validKey,
				TableName: aws.String(testTableName),
			}).Return(&dynamodb.GetItemOutput{
			Item: validKey,
		}, nil)

		actual := testStruct{}
		_, err := client.GetItem(ctx, testTableName,
			getitem.WithKey(validKey),
			getitem.AsEntity(&actual))

		assert.Nil(t, err)
		assert.Equal(t, testID, actual.ID)
		assert.Equal(t, testName, actual.Name)
	})
	t.Run("it sets all the parameters", func(t *testing.T) {
		client := setupFixture()

		m := client.awsClient.(*mockDynamoDB)
		proj := expression.NamesList(expression.Name(nameFieldName), expression.Name(idFieldName))
		expr, _ := expression.NewBuilder().WithProjection(proj).Build()

		expectedInput := &dynamodb.GetItemInput{
			Key:                      validKey,
			TableName:                aws.String(testTableName),
			ReturnConsumedCapacity:   testReturnConsumedCapacity,
			ProjectionExpression:     expr.Projection(),
			ExpressionAttributeNames: expr.Names(),
			ConsistentRead:           aws.Bool(true),
		}

		m.On("GetItem",
			ctx,
			expectedInput).Return(&dynamodb.GetItemOutput{
			Item: validKey,
		}, nil)

		actual, err := client.GetItem(ctx, testTableName,
			getitem.WithConsistentRead(aws.Bool(true)),
			getitem.WithKey(validKey),
			getitem.WithReturnConsumedCapacity(testReturnConsumedCapacity),
			getitem.WithProjectionBuilder(&proj))

		assert.Nil(t, err)
		assert.NotNil(t, actual)
		assert.Equal(t, validKey, actual.Item)
	})
}

func TestClient_ListTables(t *testing.T) {
	ctx := context.Background()
	testTableName := "test-table-name"

	t.Run("it returns an error if the aws client returns an error", func(t *testing.T) {
		client := setupFixture()

		m := client.awsClient.(*mockDynamoDB)

		expectedErr := &types.InternalServerError{
			Message: aws.String("dynamo down"),
		}

		m.On("ListTables",
			ctx, mock.Anything).Return(nil, expectedErr)

		actual, err := client.ListTables(ctx)

		assert.Nil(t, actual)
		assert.NotNil(t, err)
		assert.Equal(t, expectedErr, err)
	})
	t.Run("it calls the aws client properly", func(t *testing.T) {
		client := setupFixture()

		m := client.awsClient.(*mockDynamoDB)

		m.On("ListTables",
			ctx,
			&dynamodb.ListTablesInput{
				ExclusiveStartTableName: aws.String(testTableName),
				Limit:                   aws.Int32(42),
			}).Return(&dynamodb.ListTablesOutput{
			TableNames: []string{
				testTableName,
			},
		}, nil)

		actual, err := client.ListTables(ctx,
			listtables.WithExclusiveStartTableName(testTableName),
			listtables.WithLimit(42))

		assert.Nil(t, err)
		assert.NotNil(t, actual)
		assert.Equal(t, []string{
			testTableName,
		}, actual.TableNames)
	})
}

func TestClient_PutItem(t *testing.T) {
	ctx := context.Background()
	testTableName := "test-table-name"

	testID := "uuid1-uuid-2uu-3-uuid4"
	testName := "my item"
	testReturnConsumedCapacity := types.ReturnConsumedCapacityTotal
	testReturnItemCollectionMetrics := types.ReturnItemCollectionMetricsSize
	testReturnValues := types.ReturnValueAllOld

	validItem := map[string]types.AttributeValue{
		idFieldName:   &types.AttributeValueMemberS{Value: testID},
		nameFieldName: &types.AttributeValueMemberS{Value: testName},
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

		expectedErr := &types.InternalServerError{
			Message: aws.String("dynamo down"),
		}

		m.On("PutItem",
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

		m.On("PutItem",
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
	t.Run("it sets With Entity", func(t *testing.T) {
		client := setupFixture()

		m := client.awsClient.(*mockDynamoDB)

		m.On("PutItem",
			ctx,
			&dynamodb.PutItemInput{
				Item:      validItem,
				TableName: aws.String(testTableName),
			}).Return(&dynamodb.PutItemOutput{
			Attributes: validItem,
		}, nil)

		input := &testStruct{
			ID:   testID,
			Name: testName,
		}
		actual, err := client.PutItem(ctx, testTableName,
			putitem.WithEntity(input))

		assert.Nil(t, err)
		assert.NotNil(t, actual)
		assert.Equal(t, validItem, actual.Attributes)

		m.AssertExpectations(t)
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

		m.On("PutItem",
			ctx,
			expectedInput).Return(&dynamodb.PutItemOutput{
			Attributes: validItem,
		}, nil)

		actual, err := client.PutItem(ctx, testTableName,
			putitem.WithItem(validItem),
			putitem.WithReturnConsumedCapacity(testReturnConsumedCapacity),
			putitem.WithReturnItemCollectionMetrics(testReturnItemCollectionMetrics),
			putitem.WithReturnValue(testReturnValues),
			putitem.WithFilterConditionBuilder(&filter))

		assert.Nil(t, err)
		assert.NotNil(t, actual)
		assert.Equal(t, validItem, actual.Attributes)
	})
}

func TestClient_Query(t *testing.T) {
	ctx := context.Background()
	testTableName := "test-table-name"

	testID := "uuid1-uuid2-uuid3-uuid4"
	testName := "my item"
	testReturnConsumedCapacity := types.ReturnConsumedCapacityTotal

	t.Run("it requires a table name to be 3 or more characters", func(t *testing.T) {
		client := setupFixture()

		actual, err := client.Query(ctx, "")

		assert.Nil(t, actual)
		assert.NotNil(t, err)
		assert.Equal(t, errors.New(requiredTableNameMsg), err)

		actual, err = client.Query(ctx, "to")

		assert.Nil(t, actual)
		assert.NotNil(t, err)
		assert.Equal(t, errors.New(requiredTableNameMsg), err)
	})
	t.Run("it requires a key to be set", func(t *testing.T) {
		client := setupFixture()

		actual, err := client.Query(ctx, testTableName)

		assert.Nil(t, actual)
		assert.NotNil(t, err)
		assert.Equal(t, errors.New(requiredKeyConditionBuilderMsg), err)

		actual, err = client.Query(ctx, testTableName,
			query.WithKeyConditionBuilder(nil))

		assert.Nil(t, actual)
		assert.NotNil(t, err)
		assert.Equal(t, errors.New(requiredKeyConditionBuilderMsg), err)
	})
	t.Run("it returns an error if the aws client returns an error", func(t *testing.T) {
		client := setupFixture()

		m := client.awsClient.(*mockDynamoDB)

		expectedErr := &types.InternalServerError{
			Message: aws.String("dynamo down"),
		}

		m.On("Query",
			ctx, mock.Anything).Return(nil, expectedErr)
		keyBuilder := expression.Key(idFieldName).Equal(expression.Value(testID))

		actual, err := client.Query(ctx, testTableName,
			query.WithKeyConditionBuilder(&keyBuilder))

		assert.Nil(t, actual)
		assert.NotNil(t, err)
		assert.Equal(t, expectedErr, err)
	})
	t.Run("it calls the aws client properly", func(t *testing.T) {
		client := setupFixture()

		m := client.awsClient.(*mockDynamoDB)

		returnedItems := []map[string]types.AttributeValue{{
			idFieldName:   &types.AttributeValueMemberS{Value: testID},
			nameFieldName: &types.AttributeValueMemberS{Value: testName},
		}}
		keyBuilder := expression.Key(idFieldName).Equal(expression.Value(testID))

		expr, _ := expression.NewBuilder().WithKeyCondition(keyBuilder).Build()

		m.On("Query",
			ctx,
			&dynamodb.QueryInput{
				KeyConditionExpression:    expr.KeyCondition(),
				ExpressionAttributeNames:  expr.Names(),
				ExpressionAttributeValues: expr.Values(),
				TableName:                 aws.String(testTableName),
			}).Return(&dynamodb.QueryOutput{
			Items: returnedItems,
		}, nil)

		actual, err := client.Query(ctx, testTableName,
			query.WithKeyConditionBuilder(&keyBuilder))

		assert.Nil(t, err)
		assert.NotNil(t, actual)
		assert.Equal(t, returnedItems, actual.Items)
	})
	t.Run("it sets AsSliceOfEntities", func(t *testing.T) {
		client := setupFixture()

		m := client.awsClient.(*mockDynamoDB)

		returnedItems := []map[string]types.AttributeValue{{
			idFieldName:   &types.AttributeValueMemberS{Value: testID},
			nameFieldName: &types.AttributeValueMemberS{Value: testName},
		}}
		keyBuilder := expression.Key(idFieldName).Equal(expression.Value(testID))

		expr, _ := expression.NewBuilder().WithKeyCondition(keyBuilder).Build()

		m.On("Query",
			ctx,
			&dynamodb.QueryInput{
				KeyConditionExpression:    expr.KeyCondition(),
				ExpressionAttributeNames:  expr.Names(),
				ExpressionAttributeValues: expr.Values(),
				TableName:                 aws.String(testTableName),
			}).Return(&dynamodb.QueryOutput{
			Items: returnedItems,
		}, nil)

		actual := make([]*testStruct, 0)

		_, err := client.Query(ctx, testTableName,
			query.WithKeyConditionBuilder(&keyBuilder),
			query.AsSliceOfEntities(&actual))

		assert.Nil(t, err)
		assert.NotNil(t, actual)
		assert.Equal(t, 1, len(actual))
		assert.Equal(t, testID, actual[0].ID)
		assert.Equal(t, testName, actual[0].Name)

	})
	t.Run("it sets all the parameters", func(t *testing.T) {
		client := setupFixture()

		testScanIndexForward := true
		testSelect := types.SelectCount
		testIndexName := "GSI1"
		var testLimit int32 = 42

		m := client.awsClient.(*mockDynamoDB)
		proj := expression.NamesList(expression.Name(nameFieldName), expression.Name(idFieldName))
		key := expression.Key(idFieldName).Equal(expression.Value(testID))
		filter := expression.Name(nameFieldName).Equal(expression.Value(testName))

		expr, _ := expression.NewBuilder().WithKeyCondition(key).WithFilter(filter).WithProjection(proj).Build()

		exclusiveStartKey := map[string]types.AttributeValue{
			idFieldName: &types.AttributeValueMemberS{Value: testID},
		}

		expectedInput := &dynamodb.QueryInput{
			ConsistentRead:            aws.Bool(true),
			ExclusiveStartKey:         exclusiveStartKey,
			ExpressionAttributeNames:  expr.Names(),
			ExpressionAttributeValues: expr.Values(),
			FilterExpression:          expr.Filter(),
			IndexName:                 &testIndexName,
			KeyConditionExpression:    expr.KeyCondition(),
			Limit:                     &testLimit,
			ReturnConsumedCapacity:    testReturnConsumedCapacity,
			ProjectionExpression:      expr.Projection(),
			ScanIndexForward:          aws.Bool(testScanIndexForward),
			Select:                    testSelect,
			TableName:                 aws.String(testTableName),
		}

		returnedItems := []map[string]types.AttributeValue{{
			idFieldName:   &types.AttributeValueMemberS{Value: testID},
			nameFieldName: &types.AttributeValueMemberS{Value: testName},
		}}

		m.On("Query",
			ctx,
			expectedInput).Return(&dynamodb.QueryOutput{
			Items: returnedItems,
		}, nil)

		actual, err := client.Query(ctx, testTableName,
			query.WithConsistentRead(aws.Bool(true)),
			query.WithExclusiveStartKey(exclusiveStartKey),
			query.WithFilterConditionBuilder(&filter),
			query.WithIndexName(testIndexName),
			query.WithKeyConditionBuilder(&key),
			query.WithLimit(testLimit),
			query.WithProjectionBuilder(&proj),
			query.WithReturnConsumedCapacity(testReturnConsumedCapacity),
			query.WithScanIndexForward(testScanIndexForward),
			query.WithSelect(testSelect))

		assert.Nil(t, err)
		assert.NotNil(t, actual)

	})

}

func TestClient_Scan(t *testing.T) {
	ctx := context.Background()
	testTableName := "test-table-name"

	testID := "uuid1-uuid2-uuid3-uuid4"
	testName := "my item"
	testReturnConsumedCapacity := types.ReturnConsumedCapacityTotal

	t.Run("it requires a table name to be 3 or more characters", func(t *testing.T) {
		client := setupFixture()

		actual, err := client.Scan(ctx, "")

		assert.Nil(t, actual)
		assert.NotNil(t, err)
		assert.Equal(t, errors.New(requiredTableNameMsg), err)

		actual, err = client.Scan(ctx, "to")

		assert.Nil(t, actual)
		assert.NotNil(t, err)
		assert.Equal(t, errors.New(requiredTableNameMsg), err)
	})
	t.Run("it returns an error if the aws client returns an error", func(t *testing.T) {
		client := setupFixture()

		m := client.awsClient.(*mockDynamoDB)

		expectedErr := &types.InternalServerError{
			Message: aws.String("dynamo down"),
		}

		m.On("Scan",
			ctx, mock.Anything).Return(nil, expectedErr)

		actual, err := client.Scan(ctx, testTableName)

		assert.Nil(t, actual)
		assert.NotNil(t, err)
		assert.Equal(t, expectedErr, err)
	})
	t.Run("it calls the aws client properly", func(t *testing.T) {
		client := setupFixture()

		m := client.awsClient.(*mockDynamoDB)

		returnedItems := []map[string]types.AttributeValue{{
			idFieldName:   &types.AttributeValueMemberS{Value: testID},
			nameFieldName: &types.AttributeValueMemberS{Value: testName},
		}}

		m.On("Scan",
			ctx,
			&dynamodb.ScanInput{
				TableName: aws.String(testTableName),
			}).Return(&dynamodb.ScanOutput{
			Items: returnedItems,
		}, nil)

		actual, err := client.Scan(ctx, testTableName)

		assert.Nil(t, err)
		assert.NotNil(t, actual)
		assert.Equal(t, returnedItems, actual.Items)
	})
	t.Run("it sets all the parameters", func(t *testing.T) {
		client := setupFixture()

		testSelect := types.SelectCount
		testIndexName := "GSI1"
		var testSegment int32 = 2
		var testTotalSegments int32 = 42
		var testLimit int32 = 42

		m := client.awsClient.(*mockDynamoDB)

		proj := expression.NamesList(expression.Name(nameFieldName), expression.Name(idFieldName))
		filter := expression.Name(nameFieldName).Equal(expression.Value(testName))

		expr, _ := expression.NewBuilder().WithFilter(filter).WithProjection(proj).Build()

		exclusiveStartKey := map[string]types.AttributeValue{
			idFieldName: &types.AttributeValueMemberS{Value: testID},
		}

		expectedInput := &dynamodb.ScanInput{
			ConsistentRead:            aws.Bool(true),
			ExclusiveStartKey:         exclusiveStartKey,
			ExpressionAttributeNames:  expr.Names(),
			ExpressionAttributeValues: expr.Values(),
			FilterExpression:          expr.Filter(),
			IndexName:                 &testIndexName,
			Limit:                     &testLimit,
			ReturnConsumedCapacity:    testReturnConsumedCapacity,
			ProjectionExpression:      expr.Projection(),
			Segment:                   &testSegment,
			Select:                    testSelect,
			TableName:                 aws.String(testTableName),
			TotalSegments:             &testTotalSegments,
		}

		returnedItems := []map[string]types.AttributeValue{{
			idFieldName:   &types.AttributeValueMemberS{Value: testID},
			nameFieldName: &types.AttributeValueMemberS{Value: testName},
		}}

		m.On("Scan",
			ctx,
			expectedInput).Return(&dynamodb.ScanOutput{
			Items: returnedItems,
		}, nil)

		actual, err := client.Scan(ctx, testTableName,
			scan.WithConsistentRead(aws.Bool(true)),
			scan.WithExclusiveStartKey(exclusiveStartKey),
			scan.WithFilterConditionBuilder(&filter),
			scan.WithIndexName(testIndexName),
			scan.WithLimit(testLimit),
			scan.WithProjectionBuilder(&proj),
			scan.WithReturnConsumedCapacity(testReturnConsumedCapacity),
			scan.WithSelect(testSelect),
			scan.WithSegment(testSegment),
			scan.WithTotalSegments(testTotalSegments))

		assert.Nil(t, err)
		assert.NotNil(t, actual)

	})

}
