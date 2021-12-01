package scan

import (
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

type Options struct {
	ConsistentRead         *bool
	ExclusiveStartKey      map[string]types.AttributeValue
	FilterConditionBuilder *expression.ConditionBuilder
	IndexName              *string
	Limit                  *int32
	ProjectionBuilder      *expression.ProjectionBuilder
	ReturnConsumedCapacity types.ReturnConsumedCapacity
	Segment                *int32
	Select                 types.Select
	TotalSegments          *int32
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
		options.IndexName = &input
	}
}

func WithLimit(input int32) OptionFunc {
	return func(options *Options) {
		options.Limit = &input
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

func WithSegment(input int32) OptionFunc {
	return func(options *Options) {
		options.Segment = &input
	}
}

func WithSelect(input types.Select) OptionFunc {
	return func(options *Options) {
		options.Select = input
	}
}

func WithTotalSegments(input int32) OptionFunc {
	return func(options *Options) {
		options.TotalSegments = &input
	}
}
