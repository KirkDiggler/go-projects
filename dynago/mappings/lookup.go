package mappings

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/KirkDiggler/go-projects/tools/dynago/entities"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

const (
	requiredLookupConfig         = "mappings.NewLookup requires a LookupConfig"
	requiredLookupMappingNameMsg = "mappings.NewLookup requires LookupConfig.MappingName to be set"
	requiredLookupFieldsMsg      = "mappings.NewLookup requires LookupConfig.Fields to be set"
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
	if cfg == nil {
		return nil, errors.New(requiredLookupConfig)
	}

	if strings.TrimSpace(cfg.MappingName) == "" {
		return nil, errors.New(requiredLookupMappingNameMsg)
	}

	if len(cfg.Fields) == 0 {
		return nil, errors.New(requiredLookupFieldsMsg)
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

func (m *lookupMapping) buildValues(ctx context.Context, values map[string]types.AttributeValue) (string, error) {
	var sb = strings.Builder{}
	for _, field := range m.fields {
		if _, ok := values[field]; !ok {
			return "", fmt.Errorf("required field '%s' was not found in provided values", field)
		}

		_, err := fmt.Fprintf(&sb, "%s%s%s",
			setCasing(field),
			getFieldSeparator(),
			setCasing(attributeValueToString(values[field])))
		if err != nil {
			return "", fmt.Errorf("error returned when formatting partition field. original error: %s", err)
		}
	}

	return sb.String(), nil
}

func (m *lookupMapping) BuildPartitionValues(ctx context.Context, values map[string]types.AttributeValue) (string, error) {
	return m.buildValues(ctx, values)
}

func (m *lookupMapping) BuildSortValues(ctx context.Context, values map[string]types.AttributeValue) (string, error) {
	return m.buildValues(ctx, values)
}

func (m *lookupMapping) GetName() string {
	return m.mappingName
}

func (m *lookupMapping) GetType() entities.MappingType {
	return entities.MappingType_Lookup
}

func (m *lookupMapping) GetPartitionFields() []string {
	return m.fields
}

func (m *lookupMapping) GetSortFields() []string {
	return m.fields
}
