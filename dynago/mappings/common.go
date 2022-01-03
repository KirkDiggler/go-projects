package mappings

import (
	"strconv"
	"strings"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

func getFieldSeparator() string {
	return "#"
}

// TODO: integrate into a configurable option (possibly a plugin)
func setCasing(value string) string {
	return strings.ToUpper(value)
}

func attributeValueToString(value types.AttributeValue) string {
	switch value := value.(type) {
	case *types.AttributeValueMemberS:
		return value.Value
	case *types.AttributeValueMemberBOOL:
		return strconv.FormatBool(value.Value)
	default:
		return ""
	}
}
