package repositories

import (
	"errors"
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

	})
	t.Run("it does not allow index partition fields to be changed", func(t *testing.T) {

	})
	t.Run("it only allows sort fields to be added to the end", func(t *testing.T) {

	})
	t.Run("it adds a new mapping to an available Dynamo index", func(t *testing.T) {

	})

}
