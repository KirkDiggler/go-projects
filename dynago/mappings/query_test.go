package mappings

import (
	"context"
	"errors"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/stretchr/testify/assert"
)

func TestNewQuery(t *testing.T) {
	validMappingName := "myMapping"
	invalidFields := []string{}
	validPartitionFields := []string{"category"}
	validSortFields := []string{"name"}

	t.Run("it requires a config", func(t *testing.T) {
		_, err := NewQuery(nil)

		assert.Equal(t, errors.New(requiredQueryConfig), err)
	})
	t.Run("it requires a MappingName to be set", func(t *testing.T) {
		_, err := NewQuery(&QueryConfig{})

		assert.Equal(t, errors.New(requiredQueryMappingNameMsg), err)

		_, err = NewQuery(&QueryConfig{
			MappingName: "",
		})

		assert.Equal(t, errors.New(requiredQueryMappingNameMsg), err)

		_, err = NewQuery(&QueryConfig{
			MappingName: "  ",
		})

		assert.Equal(t, errors.New(requiredQueryMappingNameMsg), err)
	})
	t.Run("it requires PartitionFields to be set", func(t *testing.T) {
		_, err := NewQuery(&QueryConfig{
			MappingName: validMappingName,
		})

		assert.Equal(t, errors.New(requiredQueryPartitionFieldsMsg), err)

		_, err = NewQuery(&QueryConfig{
			MappingName:     validMappingName,
			PartitionFields: invalidFields,
		})

		assert.Equal(t, errors.New(requiredQueryPartitionFieldsMsg), err)
	})
	t.Run("it requires SortFields to be set", func(t *testing.T) {
		_, err := NewQuery(&QueryConfig{
			MappingName:     validMappingName,
			PartitionFields: validPartitionFields,
		})

		assert.NotNil(t, err)
		assert.Equal(t, errors.New(requiredQuerySortFieldsMsg), err)

		_, err = NewQuery(&QueryConfig{
			MappingName:     validMappingName,
			PartitionFields: validPartitionFields,
			SortFields:      invalidFields,
		})

		assert.NotNil(t, err)
		assert.Equal(t, errors.New(requiredQuerySortFieldsMsg), err)

	})
	t.Run("it returns a valid Query", func(t *testing.T) {
		actual, err := NewQuery(&QueryConfig{
			MappingName:     validMappingName,
			PartitionFields: validPartitionFields,
			SortFields:      validSortFields,
		})

		assert.Nil(t, err)
		assert.NotNil(t, actual)

		assert.Equal(t, validPartitionFields, actual.(*query).partitionFields)
		assert.Equal(t, validSortFields, actual.(*query).sortFields)
		assert.Equal(t, validMappingName, actual.(*query).mappingName)
	})
}

func TestQuery_BuildPartitionValues(t *testing.T) {
	ctx := context.Background()
	fixture, _ := NewQuery(&QueryConfig{
		MappingName:     "query-by-category-name",
		PartitionFields: []string{"category"},
		SortFields:      []string{"name"},
	})

	invalidValues := map[string]types.AttributeValue{
		"Category": &types.AttributeValueMemberS{Value: "sneakers"},
		"Name":     &types.AttributeValueMemberS{Value: "Red October"},
	}
	validValues := map[string]types.AttributeValue{
		"category": &types.AttributeValueMemberS{Value: "sneakers"},
		"name":     &types.AttributeValueMemberS{Value: "Red October"},
	}

	t.Run("it requires all the fields to be set", func(t *testing.T) {

		_, err := fixture.BuildPartitionValues(ctx, invalidValues)

		assert.Equal(t, errors.New("required field 'category' was not found in provided values"), err)
	})
	t.Run("it builds the partition values", func(t *testing.T) {
		actual, err := fixture.BuildPartitionValues(ctx, validValues)

		expected := "CATEGORY#SNEAKERS"

		assert.Nil(t, err)
		assert.NotNil(t, actual)

		assert.Equal(t, expected, actual)
	})
}

func TestQuery_BuildSortValues(t *testing.T) {
	ctx := context.Background()
	fixture, _ := NewQuery(&QueryConfig{
		MappingName:     "query-by-category-name",
		PartitionFields: []string{"category"},
		SortFields:      []string{"name", "updated_at"},
	})

	missingFirstSortField := map[string]types.AttributeValue{
		"category":   &types.AttributeValueMemberS{Value: "sneakers"},
		"updated_at": &types.AttributeValueMemberS{Value: "2019-03-07"},
	}
	firstSortField := map[string]types.AttributeValue{
		"category": &types.AttributeValueMemberS{Value: "sneakers"},
		"name":     &types.AttributeValueMemberS{Value: "Red October"},
	}
	allFieldsPresent := map[string]types.AttributeValue{
		"category":   &types.AttributeValueMemberS{Value: "sneakers"},
		"name":       &types.AttributeValueMemberS{Value: "Red October"},
		"updated_at": &types.AttributeValueMemberS{Value: "2019-03-07"},
	}

	t.Run("it requires the fields to be set in order", func(t *testing.T) {

		actual, err := fixture.BuildSortValues(ctx, missingFirstSortField)

		assert.Nil(t, err)
		assert.NotNil(t, actual)

		assert.Equal(t, "", actual)

		actual, err = fixture.BuildSortValues(ctx, firstSortField)

		expected := "NAME#RED OCTOBER"

		assert.Nil(t, err)
		assert.NotNil(t, actual)

		assert.Equal(t, expected, actual)

		actual, err = fixture.BuildSortValues(ctx, allFieldsPresent)

		expected = "NAME#RED OCTOBER#UPDATED_AT#2019-03-07"

		assert.Nil(t, err)
		assert.NotNil(t, actual)

		assert.Equal(t, expected, actual)
	})
}
