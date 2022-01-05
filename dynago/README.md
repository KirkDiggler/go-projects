# Dynago

Map an entity to a repository setting 1 or more fields to a given dynamo partition or sort key

## Terms
* Keys: Dynamo indexes values found in keys. There are two types of keys, partition and sort. Partition keys are required, sort keys are optional and allow a set of operations like begins with, less than, greater than etc.
* Fields: structs in Golang have fields that contain typed values

## Goals
1. Leverage the power of single table design by mapping entity fields to partition and sort keys in dynamo tables and indexes.
2. Allow multiple entity fields to be mapped to a specific partition or sort key.
3. Return contextualized error messages.
4. Emit metrics about usage and details of the system.

## Introduction
Dynago is a way to generate a repository based on a given entity. We will setup mappings of your entity fields to the underlying Dynamo schema.

Dynago leverages Dynamo's partition key and sort key for the table and all indexes. Dynago maps Entities to the Dynamo schema using 3 types of mappings, List, Lookup and Query.
1. List mapping puts the same value in the partition key <sup>1</sup> and allows you to assign fields to the sort key that you can search.
2. Lookup mapping puts the same field(s) in the partition and sort key. When assigned to the table mapping these are the unique value that represents a single entity.
3. Query mapping allows independent setting of the partition fields, and the sort fields

## Thoughts
Thinking of the underlying table as capable of storing anything what can we store beyoind the direct entity.

Relationships?

How do we make the ORM as lightweight and as opt in as possible.

At its core it should just save an entity to a single Dynamo table based on given mappings.

Are repositories the way? 

<sup>1</sup> List mappings can assign a configurable range of prefixes to attach to a list mappings partition keys to prevent hot sharding. This will add to the RCU when querying the mapping.