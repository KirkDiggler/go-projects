package entities

type Mapping struct {
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
