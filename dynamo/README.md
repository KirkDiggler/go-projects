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