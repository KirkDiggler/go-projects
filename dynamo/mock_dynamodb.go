package dynamo

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/stretchr/testify/mock"
)

type mockDynamoDB struct {
	mock.Mock
}

func (m *mockDynamoDB) PutItem(ctx context.Context, in *dynamodb.PutItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error) {
	args := m.Called(ctx, in)

	if args.Error(1) != nil {
		return nil, args.Error(1)
	}

	return args.Get(0).(*dynamodb.PutItemOutput), nil
}

func (m *mockDynamoDB) DeleteItem(ctx context.Context, in *dynamodb.DeleteItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.DeleteItemOutput, error) {
	args := m.Called(ctx, in)

	if args.Error(1) != nil {
		return nil, args.Error(1)
	}

	return args.Get(0).(*dynamodb.DeleteItemOutput), nil
}

func (m *mockDynamoDB) GetItem(ctx context.Context, in *dynamodb.GetItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error) {
	args := m.Called(ctx, in)

	if args.Error(1) != nil {
		return nil, args.Error(1)
	}

	return args.Get(0).(*dynamodb.GetItemOutput), nil
}

func (m *mockDynamoDB) DescribeTable(ctx context.Context, in *dynamodb.DescribeTableInput, optFns ...func(*dynamodb.Options)) (*dynamodb.DescribeTableOutput, error) {
	args := m.Called(ctx, in)

	if args.Error(1) != nil {
		return nil, args.Error(1)
	}

	return args.Get(0).(*dynamodb.DescribeTableOutput), nil
}

func (m *mockDynamoDB) ListTables(ctx context.Context, in *dynamodb.ListTablesInput, optFns ...func(*dynamodb.Options)) (*dynamodb.ListTablesOutput, error) {
	args := m.Called(ctx, in)

	if args.Error(1) != nil {
		return nil, args.Error(1)
	}

	return args.Get(0).(*dynamodb.ListTablesOutput), nil
}

func (m *mockDynamoDB) Query(ctx context.Context, in *dynamodb.QueryInput, optFns ...func(*dynamodb.Options)) (*dynamodb.QueryOutput, error) {
	args := m.Called(ctx, in)

	if args.Error(1) != nil {
		return nil, args.Error(1)
	}

	return args.Get(0).(*dynamodb.QueryOutput), nil
}

func (m *mockDynamoDB) Scan(ctx context.Context, in *dynamodb.ScanInput, optFns ...func(*dynamodb.Options)) (*dynamodb.ScanOutput, error) {
	args := m.Called(ctx, in)

	if args.Error(1) != nil {
		return nil, args.Error(1)
	}

	return args.Get(0).(*dynamodb.ScanOutput), nil
}
