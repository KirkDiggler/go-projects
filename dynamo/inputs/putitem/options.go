package putitem

import (
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/expression"
)

type Options struct {
	// input = expression.NewBuilder().WithFilter(FilterConditionBuilder)
	//
	// ConditionExpression = input.Filter()
	// ExpressionAttributeNames = input.Names()
	// ExpressionAttributeValues = input.Values()
	FilterConditionBuilder *expression.ConditionBuilder

	// maps to PutItemInput.Item
	Item map[string]*dynamodb.AttributeValue

	// maps to PutItemInput.ReturnConsumedCapacity
	ReturnConsumedCapacity *string

	// maps to PutItemInput.ReturnItemCollectionMetrics
	ReturnItemCollectionMetrics *string

	// maps to PutItemInput.ReturnValues
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

func WithItem(input map[string]*dynamodb.AttributeValue) OptionFunc {
	return func(options *Options) {
		options.Item = input
	}
}

func WithFilterConditionBuilder(input *expression.ConditionBuilder) OptionFunc {
	return func(options *Options) {
		options.FilterConditionBuilder = input
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
