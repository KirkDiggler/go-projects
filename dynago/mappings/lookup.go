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

type lookup struct {
	mappingName string
	fieldMap    map[string]bool
	fields      []string
}

type LookupConfig struct {
	MappingName string
	Fields      []string
}

func NewLookup(cfg *LookupConfig) (Interface, error) {
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

	return &lookup{
		mappingName: cfg.MappingName,
		fieldMap:    fieldMap,
		fields:      cfg.Fields,
	}, nil
}

func (m *lookup) buildValues(ctx context.Context, values map[string]types.AttributeValue) (string, error) {
	var sb = strings.Builder{}
	for _, field := range m.fields {
		if _, ok := values[field]; !ok {
			return "", fmt.Errorf("required field '%s' was not found in provided values", field)
		}

		_, err := fmt.Fprintf(&sb, "%s%s%s%s",
			setCasing(field),
			getFieldSeparator(),
			setCasing(attributeValueToString(values[field])),
			getFieldSeparator())
		if err != nil {
			return "", fmt.Errorf("error returned when formatting partition field. original error: %s", err)
		}
	}

	return sb.String()[:sb.Len()-1], nil
}

func (m *lookup) BuildPartitionValues(ctx context.Context, values map[string]types.AttributeValue) (string, error) {
	return m.buildValues(ctx, values)
}

func (m *lookup) BuildSortValues(ctx context.Context, values map[string]types.AttributeValue) (string, error) {
	return m.buildValues(ctx, values)
}

func (m *lookup) GetName() string {
	return m.mappingName
}

func (m *lookup) GetType() entities.MappingType {
	return entities.MappingType_Lookup
}

func (m *lookup) GetPartitionFields() []string {
	return m.fields
}

func (m *lookup) GetSortFields() []string {
	return m.fields
}

func (m *lookup) ToEntity() *entities.Mapping {
	return &entities.Mapping{
		Name:            m.mappingName,
		Type:            entities.MappingType_Lookup,
		PartitionFields: m.fields,
		SortFields:      m.fields,
	}
}
