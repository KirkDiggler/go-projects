package listtables

import "github.com/aws/aws-sdk-go-v2/aws"

type Options struct {
	ExclusiveStartTableName *string
	Limit                   *int32
}

type OptionFunc func(*Options)

func NewOptions(input ...OptionFunc) *Options {
	options := &Options{}

	for _, optionFunc := range input {
		optionFunc(options)
	}

	return options
}

func WithExclusiveStartTableName(input string) OptionFunc {
	return func(options *Options) {
		if input != "" {
			options.ExclusiveStartTableName = aws.String(input)
		}
	}
}

func WithLimit(input int32) OptionFunc {
	return func(options *Options) {
		if input != 0 {
			options.Limit = aws.Int32(input)
		}
	}
}
