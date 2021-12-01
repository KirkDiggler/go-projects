package query

import (
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

type Options struct {
	ConsistentRead         *bool
	ExclusiveStartKey      map[string]types.AttributeValue
	FilterConditionBuilder *expression.ConditionBuilder
	IndexName              *string
	KeyConditionBuilder    *expression.KeyConditionBuilder
	Limit                  *int32
	ProjectionBuilder      *expression.ProjectionBuilder
	ReturnConsumedCapacity types.ReturnConsumedCapacity
	ScanIndexForward       *bool
	Select                 types.Select
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

func WithExclusiveStartKey(input map[string]types.AttributeValue) OptionFunc {
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

func WithLimit(input int32) OptionFunc {
	return func(options *Options) {
		options.Limit = aws.Int32(input)
	}
}

func WithProjectionBuilder(input *expression.ProjectionBuilder) OptionFunc {
	return func(options *Options) {
		options.ProjectionBuilder = input
	}
}

func WithReturnConsumedCapacity(input types.ReturnConsumedCapacity) OptionFunc {
	return func(options *Options) {
		options.ReturnConsumedCapacity = input
	}
}

func WithScanIndexForward(input bool) OptionFunc {
	return func(options *Options) {
		options.ScanIndexForward = aws.Bool(input)
	}
}

func WithSelect(input types.Select) OptionFunc {
	return func(options *Options) {
		options.Select = input
	}
}
