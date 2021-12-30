package mappings

import (
	"context"
	"errors"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

type lookupMapping struct {
	mappingName string
	fieldMap    map[string]bool
	fields      []string
}

type LookupConfig struct {
	MappingName string
	Fields      []string
}

func NewLookup(cfg *LookupConfig) (*lookupMapping, error) {
	if cfg.MappingName == "" {
		return nil, errors.New("Lookup lookupMapping requires a lookupMapping name to be set")
	}

	if len(cfg.Fields) == 0 {
		return nil, errors.New("Lookup lookupMapping requires at least 1 field set")
	}

	fieldMap := make(map[string]bool)

	for _, field := range cfg.Fields {
		fieldMap[field] = true
	}

	return &lookupMapping{
		mappingName: cfg.MappingName,
		fieldMap:    fieldMap,
		fields:      cfg.Fields,
	}, nil
}

func (m *lookupMapping) BuildPartitionValues(ctx context.Context, values map[string]*types.AttributeValue) (string, error) {
	return "", nil
}

func (m *lookupMapping) BuildSortValues(ctx context.Context, values map[string]*types.AttributeValue) (string, error) {
	return "", nil
}

func (m *lookupMapping) GetPartitionFields() []string {
	return m.fields
}

func (m *lookupMapping) GetSortFields() []string {
	return m.fields
}
