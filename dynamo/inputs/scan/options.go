package scan

import (
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/expression"
)

type Options struct {
	ConsistentRead         *bool
	ExclusiveStartKey      map[string]*dynamodb.AttributeValue
	FilterConditionBuilder *expression.ConditionBuilder
	IndexName              *string
	Limit                  *int64
	ProjectionBuilder      *expression.ProjectionBuilder
	ReturnConsumedCapacity *string
	Segment                *int64
	Select                 *string
	TotalSegments          *int64
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
		options.IndexName = &input
	}
}

func WithLimit(input int64) OptionFunc {
	return func(options *Options) {
		options.Limit = &input
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

func WithSegment(input int64) OptionFunc {
	return func(options *Options) {
		options.Segment = &input
	}
}

func WithSelect(input string) OptionFunc {
	return func(options *Options) {
		options.Select = &input
	}
}

func WithTotalSegments(input int64) OptionFunc {
	return func(options *Options) {
		options.TotalSegments = &input
	}
}
