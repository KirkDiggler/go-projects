package repositories

import (
	"errors"
	"fmt"
	"testing"

	"github.com/KirkDiggler/go-projects/tools/dynago/entities"

	"github.com/KirkDiggler/go-projects/dynamo"
	"github.com/KirkDiggler/go-projects/tools/dynago/mappings"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/assert"
)

func TestNew(t *testing.T) {
	testTableDesc := &types.TableDescription{
		KeySchema: []types.KeySchemaElement{{
			AttributeName: aws.String("pk"),
			KeyType:       types.KeyTypeHash,
		}, {
			AttributeName: aws.String("sk"),
			KeyType:       types.KeyTypeRange,
		}},
		TableName: aws.String("my-table"),
	}

	index1Name := "GSI1pk-GSI1sk-Index"
	index2Name := "GSI2pk-GSI2sk-Index"

	gsi1Index := types.GlobalSecondaryIndexDescription{
		IndexName: aws.String(index1Name),
		Projection: &types.Projection{
			ProjectionType: types.ProjectionTypeAll,
		},
		KeySchema: []types.KeySchemaElement{{
			AttributeName: aws.String("GSI1pk"),
			KeyType:       types.KeyTypeHash,
		}, {
			AttributeName: aws.String("GSI1sk"),
			KeyType:       types.KeyTypeRange,
		}},
	}

	gsi2Index := types.GlobalSecondaryIndexDescription{
		IndexName: aws.String(index2Name),
		Projection: &types.Projection{
			ProjectionType: types.ProjectionTypeKeysOnly,
		},
		KeySchema: []types.KeySchemaElement{{
			AttributeName: aws.String("GSI2pk"),
			KeyType:       types.KeyTypeHash,
		}, {
			AttributeName: aws.String("GSI2sk"),
			KeyType:       types.KeyTypeRange,
		}},
	}

	oneGSITableDesc := &types.TableDescription{
		KeySchema: []types.KeySchemaElement{{
			AttributeName: aws.String("pk"),
			KeyType:       types.KeyTypeHash,
		}, {
			AttributeName: aws.String("sk"),
			KeyType:       types.KeyTypeRange,
		}},
		TableName: aws.String("my-table"),
		GlobalSecondaryIndexes: []types.GlobalSecondaryIndexDescription{
			gsi1Index,
		},
	}

	twoGSITableDesc := &types.TableDescription{
		KeySchema: []types.KeySchemaElement{{
			AttributeName: aws.String("pk"),
			KeyType:       types.KeyTypeHash,
		}, {
			AttributeName: aws.String("sk"),
			KeyType:       types.KeyTypeRange,
		}},
		TableName: aws.String("my-table"),
		GlobalSecondaryIndexes: []types.GlobalSecondaryIndexDescription{
			gsi1Index,
			gsi2Index,
		},
	}

	lookupByIDMapping, _ := mappings.NewLookup(&mappings.LookupConfig{
		MappingName: "table",
		Fields:      []string{"id"},
	})

	queryByCategoryMapping, _ := mappings.NewQuery(&mappings.QueryConfig{
		MappingName:     "queryByCategory",
		PartitionFields: []string{"category"},
		SortFields:      []string{"name"},
	})

	queryByTypeMapping, _ := mappings.NewQuery(&mappings.QueryConfig{
		MappingName:     "queryByTypeMapping",
		PartitionFields: []string{"type"},
		SortFields:      []string{"name"},
	})

	//t.Run("it assigns the schema Mapping")
	t.Run("it requires the table fields to not change", func(t *testing.T) {
		mapping, _ := mappings.NewLookup(&mappings.LookupConfig{
			MappingName: "table",
			Fields:      []string{"id"},
		})

		existing := &entities.Schema{
			Table: &entities.Mapping{
				PartitionFields: []string{"sku"},
				SortFields:      []string{"sku"},
			},
		}

		_, err := New(&Config{
			Name:          "MyEntity",
			Client:        &dynamo.Mock{},
			SchemaMapping: existing,
			TableDesc:     testTableDesc,
			TableMapping:  mapping,
		})

		assert.NotNil(t, err)
		assert.Equal(t, errors.New("table partition field mismatch with mapping sku != id"), err)
	})
	//t.Run("it assigns the schema Mapping")
	t.Run("it builds a new lookup mapping", func(t *testing.T) {
		mapping, _ := mappings.NewLookup(&mappings.LookupConfig{
			MappingName: "table",
			Fields:      []string{"id"},
		})

		_ = &entities.Schema{
			Table: &entities.Mapping{
				PartitionFields: []string{"id"},
				SortFields:      []string{"id"},
			},
		}

		actual, err := New(&Config{
			Name:          "MyEntity",
			Client:        &dynamo.Mock{},
			SchemaMapping: nil,
			TableDesc:     testTableDesc,
			TableMapping:  mapping,
		})

		assert.Nil(t, err)
		assert.NotNil(t, actual)

		assert.Equal(t, []string{"id"}, actual.(*repoImpl).schemaMapping.Table.GetSortFields())
	})
	t.Run("it does not allow more mappings than indexes", func(t *testing.T) {
		existing := &entities.Schema{
			Table: lookupByIDMapping.ToEntity(),
		}

		_, err := New(&Config{
			Name:          "MyEntity",
			Client:        &dynamo.Mock{},
			SchemaMapping: existing,
			TableDesc:     oneGSITableDesc,
			TableMapping:  lookupByIDMapping,
			IndexMappings: []*mappings.Index{{
				Name:           "",
				ProjectionType: entities.PropjectionTypeAll,
				Mapping:        queryByCategoryMapping,
			}, {
				Name:           "",
				ProjectionType: entities.PropjectionTypeKeysOnly,
				Mapping:        queryByTypeMapping,
			}},
		})

		assert.NotNil(t, err)

		assert.Equal(t, errors.New("requested index count 2 exceeds available indexes of 1"), err)
	})
	t.Run("it does not allow index partition field count to be increased", func(t *testing.T) {
		existing := &entities.Schema{
			Table: lookupByIDMapping.ToEntity(),
			Indexes: map[string]*entities.Index{
				queryByCategoryMapping.GetName(): {
					Name:           index1Name,
					ProjectionType: entities.PropjectionTypeAll,
					Mapping:        queryByCategoryMapping.ToEntity(),
				},
			},
		}

		newQueryByCategoryMapping, _ := mappings.NewQuery(&mappings.QueryConfig{
			MappingName:     "queryByCategory",
			PartitionFields: []string{"category", "status"},
			SortFields:      []string{"name"},
		})

		_, err := New(&Config{
			Name:          "MyEntity",
			Client:        &dynamo.Mock{},
			SchemaMapping: existing,
			TableDesc:     twoGSITableDesc,
			TableMapping:  lookupByIDMapping,
			IndexMappings: []*mappings.Index{{
				Name:           "",
				ProjectionType: entities.PropjectionTypeKeysOnly,
				Mapping:        newQueryByCategoryMapping,
			}},
		})

		assert.NotNil(t, err)
		assert.Equal(t, fmt.Errorf("mapping %s has changed the number of partition fields", newQueryByCategoryMapping.GetName()), err)
	})
	t.Run("it does not allow index partition fields to be changed", func(t *testing.T) {
		existing := &entities.Schema{
			Table: lookupByIDMapping.ToEntity(),
			Indexes: map[string]*entities.Index{
				queryByCategoryMapping.GetName(): {
					Name:           index1Name,
					ProjectionType: entities.PropjectionTypeAll,
					Mapping:        queryByCategoryMapping.ToEntity(),
				},
			},
		}

		newQueryByCategoryMapping, _ := mappings.NewQuery(&mappings.QueryConfig{
			MappingName:     "queryByCategory",
			PartitionFields: []string{"status"},
			SortFields:      []string{"name"},
		})

		_, err := New(&Config{
			Name:          "MyEntity",
			Client:        &dynamo.Mock{},
			SchemaMapping: existing,
			TableDesc:     twoGSITableDesc,
			TableMapping:  lookupByIDMapping,
			IndexMappings: []*mappings.Index{{
				ProjectionType: entities.PropjectionTypeKeysOnly,
				Mapping:        newQueryByCategoryMapping,
			}},
		})

		assert.NotNil(t, err)
		assert.Equal(t, fmt.Errorf("index mapping %s partition field mismatch, existing category != requested status", newQueryByCategoryMapping.GetName()), err)
	})
	t.Run("it does not allow sort fields to be changed", func(t *testing.T) {
		existing := &entities.Schema{
			Table: lookupByIDMapping.ToEntity(),
			Indexes: map[string]*entities.Index{
				queryByCategoryMapping.GetName(): {
					Name:           index1Name,
					ProjectionType: entities.PropjectionTypeAll,
					Mapping:        queryByCategoryMapping.ToEntity(),
				},
			},
		}

		newQueryByCategoryMapping, _ := mappings.NewQuery(&mappings.QueryConfig{
			MappingName:     "queryByCategory",
			PartitionFields: []string{"category"},
			SortFields:      []string{"status"},
		})

		_, err := New(&Config{
			Name:          "MyEntity",
			Client:        &dynamo.Mock{},
			SchemaMapping: existing,
			TableDesc:     twoGSITableDesc,
			TableMapping:  lookupByIDMapping,
			IndexMappings: []*mappings.Index{{
				ProjectionType: entities.PropjectionTypeKeysOnly,
				Mapping:        newQueryByCategoryMapping,
			}},
		})

		assert.NotNil(t, err)
		assert.Equal(t, fmt.Errorf("index mapping %s sort field mismatch, existing name != requested status", newQueryByCategoryMapping.GetName()), err)

	})
	t.Run("it allows sort fields to be added to the end of a GSI sk", func(t *testing.T) {
		existing := &entities.Schema{
			Table: lookupByIDMapping.ToEntity(),
			Indexes: map[string]*entities.Index{
				queryByCategoryMapping.GetName(): {
					Name:           index1Name,
					ProjectionType: entities.PropjectionTypeAll,
					Mapping:        queryByCategoryMapping.ToEntity(),
				},
			},
		}

		newQueryByCategoryMapping, _ := mappings.NewQuery(&mappings.QueryConfig{
			MappingName:     "queryByCategory",
			PartitionFields: []string{"category"},
			SortFields:      []string{"name", "status"},
		})

		actual, err := New(&Config{
			Name:          "MyEntity",
			Client:        &dynamo.Mock{},
			SchemaMapping: existing,
			TableDesc:     twoGSITableDesc,
			TableMapping:  lookupByIDMapping,
			IndexMappings: []*mappings.Index{{
				ProjectionType: entities.PropjectionTypeKeysOnly,
				Mapping:        newQueryByCategoryMapping,
			}},
		})

		assert.Nil(t, err)
		assert.NotNil(t, actual)

		assert.NotNil(t, actual.(*repoImpl).schemaMapping)
		assert.NotNil(t, actual.(*repoImpl).schemaMapping.Indexes[newQueryByCategoryMapping.GetName()])

		assert.Equal(t, newQueryByCategoryMapping.GetSortFields(), actual.(*repoImpl).schemaMapping.Indexes[newQueryByCategoryMapping.GetName()].Mapping.GetSortFields())
	})
	t.Run("it adds a new mapping to an available Dynamo index", func(t *testing.T) {
		existing := &entities.Schema{
			Table: lookupByIDMapping.ToEntity(),
		}

		actual, err := New(&Config{
			Name:          "MyEntity",
			Client:        &dynamo.Mock{},
			SchemaMapping: existing,
			TableDesc:     twoGSITableDesc,
			TableMapping:  lookupByIDMapping,
			IndexMappings: []*mappings.Index{{
				Name:           "",
				ProjectionType: entities.PropjectionTypeKeysOnly,
				Mapping:        queryByCategoryMapping,
			}},
		})

		assert.Nil(t, err)
		assert.NotNil(t, actual)

		assert.NotNil(t, actual.(*repoImpl).schemaMapping)
		assert.NotNil(t, actual.(*repoImpl).schemaMapping.Indexes[queryByCategoryMapping.GetName()])

		assert.Equal(t, index1Name, actual.(*repoImpl).schemaMapping.Indexes[queryByCategoryMapping.GetName()].Name, index1Name)
	})
}
