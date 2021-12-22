# Dynamo

## Introduction
The dynamo client provides an expressive interface for the aws-sdk-go library. This library uses variadic functions to populate Inputs used by DynamoDB.
all methods use the underlying `WithContext` methods so the text was left off this clients method names. PutItem calls the underlying PutItemWithContext provided by the aws sdk.
To populate an Item on a PutItemInput you would pass in `putitem.WithItem(myItem)`

example:

```go
client.PutItem(ctx, "myTableName",
	putitem.WithItem(validItem))
```
will send the following as an input
```go
&dynamodb.PutItemInput{
	Item:      validItem,
	TableName: aws.String("myTableName"),
	}
```

## Supported Methods
* DeleteItem
* DescribeTable
* GetItem
* ListTables
* PutItem
* Query
* Scan

## Usage
Most calls just set the input sent to dynamo. PutItem, GetItem and Query all have an additional options to use a user defined struct to populate with the results from dynamo.
### PutItem
```go
type myEntity struct {
	ID   string `dynamodbav:"id"`
	Name string `dynamodbav:"name"`
}

filter := expression.Name("id").Equal(expression.Value("uuid-uuid1-uuid2-uuid3-uuid4"))

item := map[string]types.AttributeValue{
   "id":   &types.AttributeValueMemberS{Value: "uuid-uuid1-uuid2-uuid3-uuid4"},
    "name": &types.AttributeValueMemberS{Value: "My Entity"},
}

entity := myEntity{}

result, err := client.PutItem(ctx, testTableName,
    putitem.WithItem(item),
    putitem.WithReturnConsumedCapacity(types.ReturnConsumedCapacityTotal),
    putitem.WithReturnItemCollectionMetrics(types.ReturnItemCollectionMetricsSize),
    putitem.WithReturnValue(types.ReturnValueAllOld),
    putitem.WithFilterConditionBuilder(&filter),
    putitem.WithEntity(&entity))
```

### GetItem
```go
type myEntity struct {
	ID   string `dynamodbav:"id"`
	Name string `dynamodbav:"name"`
}

proj := expression.NamesList(expression.Name(nameFieldName), expression.Name(idFieldName))

key := map[string]types.AttributeValue{
   "id":   &types.AttributeValueMemberS{Value: "uuid-uuid1-uuid2-uuid3-uuid4"},
}

entity := myEntity{}

result, err := client.GetItem(ctx, testTableName,
    getitem.WithConsistentRead(aws.Bool(true)),
    getitem.WithKey(key),
    getitem.WithReturnConsumedCapacity(types.ReturnConsumedCapacityTotal),
    getitem.WithProjectionBuilder(&proj),
    getitem.AsEntity(&entity))
```

### Query
```go
type myEntity struct {
	ID          string `dynamodbav:"id"`
	Name        string `dynamodbav:"name"`
    Brand       string `dynamodbav:"brand"`
    Category    string `dynamodbav:"category"`
}

proj := expression.NamesList(expression.Name(nameFieldName), expression.Name(idFieldName))

startKey := map[string]types.AttributeValue{
   "id":   &types.AttributeValueMemberS{Value: "uuid-uuid1-uuid2-uuid3-uuid4"},
}

key := expression.Key("category").Equal(expression.Value("shoes"))

filter := expression.Name("brand").Equal(expression.Value("nike"))

entities := mmake([]*myEntity, 0)

result, err := client.Query(ctx, testTableName,
    query.WithConsistentRead(aws.Bool(true)),
    query.WithExclusiveStartKey(startKey),
    query.WithFilterConditionBuilder(&filter),
    query.WithIndexName("query-by-category"),
    query.WithKeyConditionBuilder(&key),
    query.WithLimit(int32(42)),
    query.WithProjectionBuilder(&proj),
    query.WithReturnConsumedCapacity(types.ReturnConsumedCapacityTota),
    query.WithScanIndexForward(true),
    query.WithSelect(types.SelectAllProjectedAttributes),
    query.AsSliceOfStructs(&entities))

```
