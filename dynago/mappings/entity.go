package mappings

type Entity struct {
	PartitionFields []string `dynamodbav:"partition_fields"`
	SortFields      []string `dynamodbav:"sort_fields"`
}
