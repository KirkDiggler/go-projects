package repositories

import (
	"errors"
	"testing"

	"github.com/KirkDiggler/go-projects/dynamo"
	"github.com/KirkDiggler/go-projects/tools/dynago/mappings"
	"github.com/KirkDiggler/go-projects/tools/dynago/schemas"
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

		existing := &schemas.Mapping{
			Table: &mappings.Entity{
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
		assert.Equal(t, errors.New(""), err)
	})

}
