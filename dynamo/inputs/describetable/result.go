package describetable

import (
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

type Result struct {
	Table *types.TableDescription
}
