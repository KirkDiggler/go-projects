package repositories

import (
	"context"
	"errors"
	"fmt"

	"github.com/KirkDiggler/go-projects/tools/dynago/mappings"

	"github.com/KirkDiggler/go-projects/tools/dynago/schemas"

	"github.com/KirkDiggler/go-projects/tools/dynago/repositories/options/putitem"

	"github.com/KirkDiggler/go-projects/dynamo"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

type repoImpl struct {
	name          string
	client        dynamo.Interface
	schemaMapping *schemas.Mapping
}

type Config struct {
	Name   string
	Client dynamo.Interface

	TableDesc *types.TableDescription

	// Load the existing mapping if it exists
	SchemaMapping *schemas.Mapping

	TableMapping  mappings.Interface
	IndexMappings []mappings.Index
}

const (
	requiresConfigMsg          = "repositories.New requires a Config"
	requiresConfigNameMsg      = "repositories.Config.Name is required"
	requiresConfigTableDescMsg = "repositories.Config.TableDesc is required"
	requiresConfigTableMapping = "repositories.Config.TableMapping is required"
)

// New
//
func New(cfg *Config) (Interface, error) {
	if cfg == nil {
		return nil, errors.New(requiresConfigMsg)
	}

	if cfg.Name == "" {
		return nil, errors.New(requiresConfigNameMsg)
	}

	// TODO: should we lazily load this if it is missing?
	if cfg.TableDesc == nil {
		return nil, errors.New(requiresConfigTableDescMsg)
	}

	if cfg.TableMapping == nil {
		return nil, errors.New(requiresConfigTableMapping)
	}

	if validationErr := mappingIsValid(cfg.SchemaMapping, cfg.TableDesc, cfg.TableMapping, cfg.IndexMappings); validationErr != nil {
		return nil, validationErr
	}

	schemaMapping, err := buildMapping(cfg.SchemaMapping, cfg.TableDesc, cfg.TableMapping, cfg.IndexMappings)
	if err != nil {
		return nil, err
	}

	return &repoImpl{
		name:          cfg.Name,
		client:        cfg.Client,
		schemaMapping: schemaMapping,
	}, nil

}

func (r *repoImpl) Put(context.Context, ...func(*putitem.Options)) (*putitem.Result, error) {
	return nil, errors.New("not implemented")
}

func buildMapping(existing *schemas.Mapping, tableDesc *types.TableDescription, tableMapping mappings.Interface, indexMappings []mappings.Index) (*schemas.Mapping, error) {
	if validErr := mappingIsValid(existing, tableDesc, tableMapping, indexMappings); validErr != nil {
		return nil, validErr
	}

	if existing == nil {
		existing = &schemas.Mapping{}
	}

	if existing.Table == nil {
		return buildNewMapping(tableMapping, indexMappings), nil
	}

	// table has to match to pass validation so we move onto the indexes

}

func buildNewMapping(tableMapping mappings.Interface, indexMappings []mappings.Index) *schemas.Mapping {
	indexMap := make(map[string]mappings.Index)

	for _, index := range indexMappings {
		indexMap[index.Name] = index
	}

	return &schemas.Mapping{
		Table:   tableMapping,
		Indexes: indexMap,
	}
}

// Checks if the existing mapping is compatible with current mappings
// TableMapping must match exactly
func mappingIsValid(existing *schemas.Mapping, tableDesc *types.TableDescription, tableMapping mappings.Interface, indexMappings []mappings.Index) error {
	if existing == nil {
		return nil
	}

	if len(existing.Table.GetPartitionFields()) != len(tableMapping.GetPartitionFields()) {
		return errors.New("partition field counts do not match")
	}

	if len(existing.Table.GetSortFields()) != len(tableMapping.GetSortFields()) {
		return errors.New("sort field counts do not match")
	}

	for idx, field := range existing.Table.GetPartitionFields() {
		if field != tableMapping.GetPartitionFields()[idx] {
			return fmt.Errorf("table partrition field mismatch with mapping %s != %s", field, tableMapping.GetPartitionFields()[idx])
		}
	}

	for idx, field := range existing.Table.GetSortFields() {
		if field != tableMapping.GetSortFields()[idx] {
			return fmt.Errorf("table sort field mismatch with mapping. Existing field %s != %s", field, tableMapping.GetSortFields()[idx])
		}
	}

	for k, v := range existing.Indexes {
		found := false
		for _, index := range indexMappings {
			if v.Name == index.Name {
				found = true
				for idx, field := range v.Mapping.GetPartitionFields() {
					if field != index.Mapping.GetPartitionFields()[idx] {
						return fmt.Errorf("index %s partrition field mismatch with mapping, existing %s != %s",
							index.Name,
							field,
							tableMapping.GetPartitionFields()[idx])
					}
				}
				for idx, field := range v.Mapping.GetSortFields() {
					if field != index.Mapping.GetSortFields()[idx] {
						return fmt.Errorf("index %s sort field mismatch with mapping, existing %s != %s",
							index.Name,
							field,
							tableMapping.GetSortFields()[idx])
					}
				}
			}
		}
		if found == false {
			// assign new mapping
		}
	}

	return nil
}
