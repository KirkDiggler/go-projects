package mappings

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

type Entity interface {
	GetName() string
	GetPartitionFields() []string
	GetSortFields() []string
}

type Interface interface {
	Entity
	BuildPartitionValues(ctx context.Context, values map[string]types.AttributeValue) (string, error)
	BuildSortValues(ctx context.Context, values map[string]types.AttributeValue) (string, error)
}
