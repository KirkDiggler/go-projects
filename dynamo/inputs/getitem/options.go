package getitem

import (
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/expression"
)

type Options struct {
	// maps to GetItemInput.AttributesToGet
	AttributesToGet []*string `min:"1" type:"list"`

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
	Key map[string]*dynamodb.AttributeValue

	// maps to GetItemInput.ReturnConsumedCapacity
	ReturnConsumedCapacity *string
}

type OptionFunc func(*Options)

func NewOptions(input ...OptionFunc) *Options {
	options := &Options{}

	for _, optionFunc := range input {
		optionFunc(options)
	}

	return options
}

func WithAttributesToGet(input []*string) OptionFunc {
	return func(options *Options) {
		options.AttributesToGet = input
	}
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

func WithKey(input map[string]*dynamodb.AttributeValue) OptionFunc {
	return func(options *Options) {
		options.Key = input
	}
}

func WithReturnConsumedCapacity(input *string) OptionFunc {
	return func(options *Options) {
		options.ReturnConsumedCapacity = input
	}
}
