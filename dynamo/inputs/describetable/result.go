package describetable

import "github.com/aws/aws-sdk-go/service/dynamodb"

type Result struct {
	Table *dynamodb.TableDescription
}
