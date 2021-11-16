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
// WithItem takes a custom struct that will be converted to a map[string]*dynamodb.AttributeValue
func WithItem(item interface{}) OptionFunc {
	return func(options *Options) {
		options.Item = item
	}
}
