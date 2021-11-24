package listtables

type Result struct {
	LastEvaluatedTableName *string
	TableNames             []*string
}
