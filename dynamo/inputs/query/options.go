package query

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/expression"
)

type Options struct {
	ConsistentRead         *bool
	ExclusiveStartKey      map[string]*dynamodb.AttributeValue
	FilterConditionBuilder *expression.ConditionBuilder
	IndexName              *string
	KeyConditionBuilder    *expression.KeyConditionBuilder
	Limit                  *int64
	ProjectionBuilder      *expression.ProjectionBuilder
	ReturnConsumedCapacity *string
	ScanIndexForward       *bool
	Select                 *string
}

type OptionFunc func(*Options)

func NewOptions(input ...OptionFunc) *Options {
	options := &Options{}

	for _, optionFunc := range input {
		optionFunc(options)
	}

	return options
}

func WithConsistentRead(input *bool) OptionFunc {
	return func(options *Options) {
		options.ConsistentRead = input
	}
}

func WithExclusiveStartKey(input map[string]*dynamodb.AttributeValue) OptionFunc {
	return func(options *Options) {
		options.ExclusiveStartKey = input
	}
}

func WithFilterConditionBuilder(input *expression.ConditionBuilder) OptionFunc {
	return func(options *Options) {
		options.FilterConditionBuilder = input
	}
}

func WithIndexName(input string) OptionFunc {
	return func(options *Options) {
		options.IndexName = aws.String(input)
	}
}

func WithKeyConditionBuilder(input *expression.KeyConditionBuilder) OptionFunc {
	return func(options *Options) {
		options.KeyConditionBuilder = input
	}
}

func WithLimit(input int64) OptionFunc {
	return func(options *Options) {
		options.Limit = aws.Int64(input)
	}
}

func WithProjectionBuilder(input *expression.ProjectionBuilder) OptionFunc {
	return func(options *Options) {
		options.ProjectionBuilder = input
	}
}

func WithReturnConsumedCapacity(input *string) OptionFunc {
	return func(options *Options) {
		options.ReturnConsumedCapacity = input
	}
}

func WithScanIndexForward(input bool) OptionFunc {
	return func(options *Options) {
		options.ScanIndexForward = aws.Bool(input)
	}
}

func WithSelect(input string) OptionFunc {
	return func(options *Options) {
		options.Select = aws.String(input)
	}
}
