package deleteitem

import (
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/expression"
)

type Options struct {
	// input = expression.NewBuilder().WithFilter(ConditionalExpression)
	//
	// ConditionExpression = input.Filter()
	// ExpressionAttributeNames = input.Names()
	// ExpressionAttributeValues = input.Values()
	ConditionalExpression *expression.ConditionBuilder

	// maps to DeleteItemInput.Key
	//
	// Key is a required field
	Key map[string]*dynamodb.AttributeValue

	// maps to DeleteItemInput.ReturnConsumedCapacity
	ReturnConsumedCapacity *string

	// maps to DeleteItemInput.ReturnItemCollectionMetrics
	ReturnItemCollectionMetrics *string

	// maps to DeleteItemInput.ReturnValues
	ReturnValues *string
}

type OptionFunc func(*Options)

func NewOptions(input ...OptionFunc) *Options {
	options := &Options{}

	for _, optionFunc := range input {
		optionFunc(options)
	}

	return options
}

func WithKey(input map[string]*dynamodb.AttributeValue) OptionFunc {
	return func(options *Options) {
		options.Key = input
	}
}

func WithConditionalExpression(input *expression.ConditionBuilder) OptionFunc {
	return func(options *Options) {
		options.ConditionalExpression = input
	}
}

func WithReturnConsumedCapacity(input *string) OptionFunc {
	return func(options *Options) {
		options.ReturnConsumedCapacity = input
	}
}

func WithReturnItemCollectionMetrics(input *string) OptionFunc {
	return func(options *Options) {
		options.ReturnItemCollectionMetrics = input
	}
}

func WithReturnValues(input *string) OptionFunc {
	return func(options *Options) {
		options.ReturnValues = input
	}
}
