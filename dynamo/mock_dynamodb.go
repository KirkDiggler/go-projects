package dynamo

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/request"

	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"

	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/stretchr/testify/mock"
)

type MockDynamoDB struct {
	dynamodbiface.DynamoDBAPI
	mock.Mock
}

func (m *MockDynamoDB) PutItemWithContext(ctx aws.Context, in *dynamodb.PutItemInput, opts ...request.Option) (*dynamodb.PutItemOutput, error) {
	args := m.Called(ctx, in, opts)

	if args.Error(1) != nil {
		return nil, args.Error(1)
	}

	return args.Get(0).(*dynamodb.PutItemOutput), nil
}

func (m *MockDynamoDB) DeleteItemWithContext(ctx aws.Context, in *dynamodb.DeleteItemInput, opts ...request.Option) (*dynamodb.DeleteItemOutput, error) {
	args := m.Called(ctx, in, opts)

	if args.Error(1) != nil {
		return nil, args.Error(1)
	}

	return args.Get(0).(*dynamodb.DeleteItemOutput), nil
}

func (m *MockDynamoDB) DescribeTableWithContext(ctx aws.Context, in *dynamodb.DescribeTableInput, opts ...request.Option) (*dynamodb.DescribeTableOutput, error) {
	args := m.Called(ctx, in)

	if args.Error(1) != nil {
		return nil, args.Error(1)
	}

	return args.Get(0).(*dynamodb.DescribeTableOutput), nil
}
