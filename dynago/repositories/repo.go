package repositories

import (
	"context"
	"errors"
	"fmt"

	"github.com/KirkDiggler/go-projects/tools/dynago/entities"

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
	SchemaMapping *entities.Schema

	TableMapping  mappings.Interface
	IndexMappings []*mappings.Index
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

	schemaMapping, err := updateEntity(cfg.SchemaMapping, cfg.TableDesc, cfg.TableMapping, cfg.IndexMappings)
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

func prepareSchema(schema *entities.Schema) *entities.Schema {
	if schema == nil {
		return &entities.Schema{
			Table: &entities.Mapping{
				Name:            "table",
				PartitionFields: []string{},
				SortFields:      []string{},
			},
			Indexes: make(map[string]*entities.Index),
		}
	}

	if schema.Indexes == nil {
		schema.Indexes = make(map[string]*entities.Index)
	}

	return schema
}

func updateEntity(existingEntity *entities.Schema, tableDesc *types.TableDescription, tableMapping mappings.Interface, indexMappings []*mappings.Index) (*schemas.Mapping, error) {
	if validErr := mappingIsValid(existingEntity, tableDesc, tableMapping, indexMappings); validErr != nil {
		return nil, validErr
	}

	existingEntity = prepareSchema(existingEntity)

	// Get a list of indexes names that are not currently assigned
	var availableIndexes []string
	for _, index := range tableDesc.GlobalSecondaryIndexes {
		if index.IndexName == nil {
			continue
		}

		if _, ok := existingEntity.Indexes[*index.IndexName]; !ok {
			availableIndexes = append(availableIndexes, *index.IndexName)
		}
	}

	// table has to match to pass validation so we move onto the indexes
	// Indexes have passed partition field checks so they match,
	// we will focus on checking sort keys and new index mappings
	for _, index := range indexMappings {
		found := false

		// Scan existing indexes to see if they have a matching mapping name
		for _, v := range existingEntity.Indexes {
			if v.Mapping.Name == index.Mapping.GetName() {

				found = true
				break
			}
		}

		if !found {
			if len(availableIndexes) > 0 {
				existingEntity.Indexes[index.Mapping.GetName()] = &entities.Index{
					Name:           availableIndexes[0],
					ProjectionType: index.ProjectionType,
					Mapping: &entities.Mapping{
						Name:            index.Mapping.GetName(),
						Type:            index.Mapping.GetType(),
						PartitionFields: index.Mapping.GetPartitionFields(),
						SortFields:      index.Mapping.GetSortFields(),
					},
				}

				index.Name = availableIndexes[0]

				// pop the first one off
				availableIndexes = availableIndexes[1:]
			}
		}
	}

	return buildNewMapping(tableMapping, indexMappings), nil
}

func buildNewMapping(tableMapping mappings.Interface, indexMappings []*mappings.Index) *schemas.Mapping {
	indexMap := make(map[string]*mappings.Index)

	for _, index := range indexMappings {
		indexMap[index.Mapping.GetName()] = index
	}

	return &schemas.Mapping{
		Table:   tableMapping,
		Indexes: indexMap,
	}
}

// Checks if the existing mapping is compatible with current mappings
// TableMapping must match exactly
func mappingIsValid(existing *entities.Schema, tableDesc *types.TableDescription, tableMapping mappings.Interface, indexMappings []*mappings.Index) error {
	if existing == nil {
		return nil
	}

	tableIndexCount := len(tableDesc.GlobalSecondaryIndexes)
	requestedIndexCount := len(indexMappings)

	if tableIndexCount < requestedIndexCount {
		return fmt.Errorf("requested index count %d exceeds available indexes of %d", requestedIndexCount, tableIndexCount)
	}
	if len(existing.Table.PartitionFields) != len(tableMapping.GetPartitionFields()) {
		return errors.New("partition field counts do not match")
	}

	if len(existing.Table.SortFields) != len(tableMapping.GetSortFields()) {
		return errors.New("sort field counts do not match")
	}

	for idx, field := range existing.Table.PartitionFields {
		if field != tableMapping.GetPartitionFields()[idx] {
			return fmt.Errorf("table partition field mismatch with mapping %s != %s", field, tableMapping.GetPartitionFields()[idx])
		}
	}

	for idx, field := range existing.Table.SortFields {
		if field != tableMapping.GetSortFields()[idx] {
			return fmt.Errorf("table sort field mismatch with mapping. Existing field %s != %s", field, tableMapping.GetSortFields()[idx])
		}
	}

	for k, v := range existing.Indexes {
		found := false
		for _, index := range indexMappings {
			if len(v.Mapping.PartitionFields) != len(index.Mapping.GetPartitionFields()) {
				return fmt.Errorf("mapping %s has changed the number of partition fields", index.Mapping.GetName())
			}

			if v.Mapping.Name == index.Mapping.GetName() {
				found = true
				for idx, field := range v.Mapping.PartitionFields {
					if field != index.Mapping.GetPartitionFields()[idx] {
						return fmt.Errorf("index mapping %s partition field mismatch, existing %s != requested %s",
							index.Mapping.GetName(),
							field,
							index.Mapping.GetPartitionFields()[idx])
					}
				}

				for idx, field := range v.Mapping.SortFields {
					if field != index.Mapping.GetSortFields()[idx] {
						return fmt.Errorf("index mapping %s sort field mismatch, existing %s != requested %s",
							index.Mapping.GetName(),
							field,
							index.Mapping.GetSortFields()[idx])
					}
				}
			}
		}

		if !found {
			return fmt.Errorf("existing index mapping %s assigned to Global Secondary Index %s was not found",
				v.Name, k)
		}
	}

	return nil
}
