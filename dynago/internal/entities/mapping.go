package entities

// Mapping
//
// A mapping for a given repos access pattern  to an index or table keys.
// Unique for each Version, RepoName and Name
type Mapping struct {
	Version   string `dynamodbav:"version"`
	IndexName string `dynamodbav:"index_name"` // if set to table, then the mapping is assigned to the table

	RepoName        string   `dynamodbav:"repo_name"`
	Name            string   `dynamodbav:"name"`
	PartitionFields []string `dynamodbav:"partition_fields"`
	SortFields      []string `dynamodbav:"sort_fields"`
}

func (m *Mapping) GetName() string {
	return m.Name
}

func (m *Mapping) GetPartitionFields() []string {
	return m.PartitionFields
}

func (m *Mapping) GetSortFields() []string {
	return m.SortFields
}

func (m *Mapping) SetPartitionFields(fields []string) {
	m.PartitionFields = fields
}

func (m *Mapping) SetSortFields(fields []string) {
	m.SortFields = fields
}
