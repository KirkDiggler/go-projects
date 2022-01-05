package mappings

import (
	"context"
	"errors"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/stretchr/testify/assert"
)

func TestNewLookup(t *testing.T) {
	validMappingName := "myMapping"
	invalidFields := []string{}
	validFields := []string{"id"}

	t.Run("it requires a config", func(t *testing.T) {
		_, err := NewLookup(nil)

		assert.Equal(t, errors.New(requiredLookupConfig), err)
	})
	t.Run("it requires a MappingName to be set", func(t *testing.T) {
		_, err := NewLookup(&LookupConfig{})

		assert.Equal(t, errors.New(requiredLookupMappingNameMsg), err)

		_, err = NewLookup(&LookupConfig{
			MappingName: "",
		})

		assert.Equal(t, errors.New(requiredLookupMappingNameMsg), err)

		_, err = NewLookup(&LookupConfig{
			MappingName: "  ",
		})

		assert.Equal(t, errors.New(requiredLookupMappingNameMsg), err)
	})
	t.Run("it requires Fields to be set", func(t *testing.T) {
		_, err := NewLookup(&LookupConfig{
			MappingName: validMappingName,
		})

		assert.Equal(t, errors.New(requiredLookupFieldsMsg), err)

		_, err = NewLookup(&LookupConfig{
			MappingName: validMappingName,
			Fields:      invalidFields,
		})

		assert.Equal(t, errors.New(requiredLookupFieldsMsg), err)
	})
	t.Run("it returns a valid Lookup", func(t *testing.T) {
		actual, err := NewLookup(&LookupConfig{
			MappingName: validMappingName,
			Fields:      validFields,
		})

		assert.Nil(t, err)
		assert.NotNil(t, actual)

		assert.Equal(t, validFields, actual.(*lookup).fields)
		assert.Equal(t, validMappingName, actual.(*lookup).mappingName)
	})
}

func TestLookupMapping_BuildPartitionValues(t *testing.T) {
	ctx := context.Background()
	fixture, _ := NewLookup(&LookupConfig{
		MappingName: "lookup-by-category-name",
		Fields:      []string{"category", "name"},
	})

	invalidValues := map[string]types.AttributeValue{
		"category": &types.AttributeValueMemberS{Value: "sneakers"},
		"Name":     &types.AttributeValueMemberS{Value: "Red October"},
	}
	validValues := map[string]types.AttributeValue{
		"category": &types.AttributeValueMemberS{Value: "sneakers"},
		"name":     &types.AttributeValueMemberS{Value: "Red October"},
	}

	t.Run("it requires all the fields to be set", func(t *testing.T) {

		_, err := fixture.BuildPartitionValues(ctx, invalidValues)

		assert.Equal(t, errors.New("required field 'name' was not found in provided values"), err)
	})
	t.Run("it builds the partition values", func(t *testing.T) {
		actual, err := fixture.BuildPartitionValues(ctx, validValues)

		expected := "CATEGORY#SNEAKERS#NAME#RED OCTOBER"

		assert.Nil(t, err)
		assert.NotNil(t, actual)

		assert.Equal(t, expected, actual)
	})
}

func TestLookupMapping_BuildSortValues(t *testing.T) {
	ctx := context.Background()
	fixture, _ := NewLookup(&LookupConfig{
		MappingName: "lookup-by-category-name",
		Fields:      []string{"category", "name"},
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

		_, err := fixture.BuildSortValues(ctx, invalidValues)

		assert.Equal(t, errors.New("required field 'category' was not found in provided values"), err)
	})
	t.Run("it builds the partition values", func(t *testing.T) {
		actual, err := fixture.BuildSortValues(ctx, validValues)

		expected := "CATEGORY#SNEAKERS#NAME#RED OCTOBER"

		assert.Nil(t, err)
		assert.NotNil(t, actual)

		assert.Equal(t, expected, actual)
	})
}

func TestLookupMapping_GetName(t *testing.T) {
	t.Run("it returns the configured name", func(t *testing.T) {
		fields := []string{"category", "name"}
		mappingName := "lookup-by-category-name"

		fixture, _ := NewLookup(&LookupConfig{
			MappingName: mappingName,
			Fields:      fields,
		})

		assert.Equal(t, mappingName, fixture.GetName())
	})
}

func TestLookupMapping_GetPartitionFields(t *testing.T) {
	t.Run("it returns the configured fields", func(t *testing.T) {
		fields := []string{"category", "name"}
		mappingName := "lookup-by-category-name"

		fixture, _ := NewLookup(&LookupConfig{
			MappingName: mappingName,
			Fields:      fields,
		})

		assert.Equal(t, fields, fixture.GetPartitionFields())
	})
}

func TestLookupMapping_GetSortFields(t *testing.T) {
	t.Run("it returns the configured fields", func(t *testing.T) {
		fields := []string{"category", "name"}
		mappingName := "lookup-by-category-name"

		fixture, _ := NewLookup(&LookupConfig{
			MappingName: mappingName,
			Fields:      fields,
		})

		assert.Equal(t, fields, fixture.GetSortFields())
	})
}
