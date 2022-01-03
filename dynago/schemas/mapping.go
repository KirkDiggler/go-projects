package schemas

import "github.com/KirkDiggler/go-projects/tools/dynago/mappings"

// Maps repository indexes to dynamo table and indexes

// Mapping
//
// Maps the table schema to a repositories mapping and is persisted for validation of changes
type Mapping struct {
	Table mappings.Entity

	// Indexes is a map[IndexName] to dynago mapping
	Indexes map[string]mappings.Index
}
