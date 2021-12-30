package putitem

import "github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression"

type Options struct {
	Entity interface{}
	FilterConditionBuilder *expression.ConditionBuilder
}

func NewOptions(input ...func(*Options)) *Options {
	out := &Options{}
	for _, fn := range input {
		fn(out)
	}

	return out
}

func WithEntity(input interface{}) func(*Options) {
	return func(args *Options) {
		args.Entity = input
	}
}

func WithFilterConditionBuilder(input *expression.ConditionBuilder) func(*Options) {
	return func(args *Options) {
		args.FilterConditionBuilder = input
	}
}
