package putitem

import (
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

type Options struct {
	// input = expression.NewBuilder().WithFilter(FilterConditionBuilder)
	//
	// ConditionExpression = input.Filter()
	// ExpressionAttributeNames = input.Names()
	// ExpressionAttributeValues = input.Values()
	FilterConditionBuilder *expression.ConditionBuilder

	// maps to PutItemInput.Item
	Item map[string]types.AttributeValue

	// maps to PutItemInput.ReturnConsumedCapacity
	ReturnConsumedCapacity types.ReturnConsumedCapacity

	// maps to PutItemInput.ReturnItemCollectionMetrics
	ReturnItemCollectionMetrics types.ReturnItemCollectionMetrics

	// maps to PutItemInput.ReturnValue
	ReturnValue types.ReturnValue
}

type OptionFunc func(*Options)

func NewOptions(input ...OptionFunc) *Options {
	options := &Options{}

	for _, optionFunc := range input {
		optionFunc(options)
	}

	return options
}

func WithItem(input map[string]types.AttributeValue) OptionFunc {
	return func(options *Options) {
		options.Item = input
	}
}

func WithFilterConditionBuilder(input *expression.ConditionBuilder) OptionFunc {
	return func(options *Options) {
		options.FilterConditionBuilder = input
	}
}

func WithReturnConsumedCapacity(input types.ReturnConsumedCapacity) OptionFunc {
	return func(options *Options) {
		options.ReturnConsumedCapacity = input
	}
}

func WithReturnItemCollectionMetrics(input types.ReturnItemCollectionMetrics) OptionFunc {
	return func(options *Options) {
		options.ReturnItemCollectionMetrics = input
	}
}

func WithReturnValue(input types.ReturnValue) OptionFunc {
	return func(options *Options) {
		options.ReturnValue = input
	}
}
