package listtables

import "github.com/aws/aws-sdk-go/aws"

type Options struct {
	ExclusiveStartTableName *string
	Limit                   *int64
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

func WithLimit(input int64) OptionFunc {
	return func(options *Options) {
		if input != 0 {
			options.Limit = aws.Int64(input)
		}
	}
}
