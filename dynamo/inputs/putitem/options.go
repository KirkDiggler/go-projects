package putitem

type Options struct {
	Item interface{}
}

type OptionFunc func(*Options)

func NewOptions(input ...OptionFunc) *Options {
	options := &Options{}

	for _, optionFunc := range input {
		optionFunc(options)
	}

	return options
}

// WithItem
//
// WithItem marshals the input to a map[string]*dynamodb.AttributeValue using dynamodbattribute package
func WithItem(item interface{}) OptionFunc {
	return func(options *Options) {
		options.Item = item
	}
}
