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
	requiredQueryConfig             = "mappings.NewQuery requires a QueryConfig"
	requiredQueryMappingNameMsg     = "mappings.NewQuery requires QueryConfig.MappingName to be set"
	requiredQueryPartitionFieldsMsg = "mappings.NewQuery requires QueryConfig.PartitionFields to be set"
	requiredQuerySortFieldsMsg      = "mappings.NewQuery requires QueryConfig.SortFields to be set"
)

type query struct {
	mappingName       string
	partitionFieldMap map[string]bool
	sortFieldMap      map[string]bool

	partitionFields []string
	sortFields      []string
}

type QueryConfig struct {
	MappingName     string
	PartitionFields []string
	SortFields      []string
}

func NewQuery(cfg *QueryConfig) (Interface, error) {
	if cfg == nil {
		return nil, errors.New(requiredQueryConfig)
	}

	if strings.TrimSpace(cfg.MappingName) == "" {
		return nil, errors.New(requiredQueryMappingNameMsg)
	}

	if len(cfg.PartitionFields) == 0 {
		return nil, errors.New(requiredQueryPartitionFieldsMsg)
	}

	if len(cfg.SortFields) == 0 {
		return nil, errors.New(requiredQuerySortFieldsMsg)
	}

	partitionFieldMap := make(map[string]bool)

	for _, field := range cfg.PartitionFields {
		partitionFieldMap[field] = true
	}

	sortFieldMap := make(map[string]bool)

	for _, field := range cfg.SortFields {
		sortFieldMap[field] = true
	}

	return &query{
		mappingName:       cfg.MappingName,
		partitionFieldMap: partitionFieldMap,
		sortFieldMap:      sortFieldMap,
		partitionFields:   cfg.PartitionFields,
		sortFields:        cfg.SortFields,
	}, nil
}

func (m *query) BuildPartitionValues(ctx context.Context, values map[string]types.AttributeValue) (string, error) {
	var sb = strings.Builder{}
	for _, field := range m.partitionFields {
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

func (m *query) BuildSortValues(ctx context.Context, values map[string]types.AttributeValue) (string, error) {
	var sb = strings.Builder{}
	for _, field := range m.sortFields {
		if _, ok := values[field]; !ok {
			break // exit out of the first field not found. (Only build values in order)"
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

	if sb.Len() == 0 {
		return "", nil
	}

	return sb.String()[:sb.Len()-1], nil
}

func (m *query) GetName() string {
	return m.mappingName
}

func (m *query) GetType() entities.MappingType {
	return entities.MappingType_Query
}

func (m *query) GetPartitionFields() []string {
	return m.partitionFields
}

func (m *query) GetSortFields() []string {
	return m.sortFields
}

func (m *query) ToEntity() *entities.Mapping {
	return &entities.Mapping{
		Name:            m.mappingName,
		Type:            entities.MappingType_Query,
		PartitionFields: m.partitionFields,
		SortFields:      m.sortFields,
	}
}
