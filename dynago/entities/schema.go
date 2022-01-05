package entities

type Schema struct {
	Table   *Mapping          `dynamodbav:"table"`
	Indexes map[string]*Index `dynamodbav:"indexes"`
}
