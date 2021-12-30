package mappings

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

type EntityInterface interface {
	GetPartitionFields() []string
	GetSortFields() []string
}

type Interface interface {
	EntityInterface
	BuildPartitionValues(ctx context.Context, values map[string]*types.AttributeValue) (string, error)
	BuildSortValues(ctx context.Context, values map[string]*types.AttributeValue) (string, error)
}
