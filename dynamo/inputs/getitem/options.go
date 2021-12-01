package getitem

import (
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

type Options struct {
	// maps to GetItemInput.ConsistentRead
	ConsistentRead *bool `type:"boolean"`

	// input = expression.NewBuilder().WithProjection(*options.ProjectionBuilder).Build()
	//
	// ConditionExpression = input.Projection()
	// ExpressionAttributeNames = input.Names()
	ProjectionBuilder *expression.ProjectionBuilder

	// maps to GetItemInput.Key
	//
	// Key is a required field
	Key map[string]types.AttributeValue

	// maps to GetItemInput.ReturnConsumedCapacity
	ReturnConsumedCapacity types.ReturnConsumedCapacity
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

func WithConditionalExpression(input *expression.ProjectionBuilder) OptionFunc {
	return func(options *Options) {
		options.ProjectionBuilder = input
	}
}

func WithKey(input map[string]types.AttributeValue) OptionFunc {
	return func(options *Options) {
		options.Key = input
	}
}

func WithReturnConsumedCapacity(input types.ReturnConsumedCapacity) OptionFunc {
	return func(options *Options) {
		options.ReturnConsumedCapacity = input
	}
}
