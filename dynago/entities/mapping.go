package entities

type MappingType string

const (
	MappingType_Lookup = "lookup"
	MappingType_List   = "list"
	MappingType_Query  = "query"
)

type Mapping struct {
	Name            string      `dynamodbav:"name"`
	Type            MappingType `dynamodbav:"type"`
	PartitionFields []string    `dynamodbav:"partition_fields"`
	SortFields      []string    `dynamodbav:"sort_fields"`
}
