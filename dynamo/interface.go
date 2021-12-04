package dynamo

import (
	"context"

	"github.com/KirkDiggler/go-projects/dynamo/inputs/describetable"
	"github.com/KirkDiggler/go-projects/dynamo/inputs/getitem"
	"github.com/KirkDiggler/go-projects/dynamo/inputs/listtables"
	"github.com/KirkDiggler/go-projects/dynamo/inputs/putitem"
	"github.com/KirkDiggler/go-projects/dynamo/inputs/query"
	"github.com/KirkDiggler/go-projects/dynamo/inputs/scan"

	"github.com/KirkDiggler/go-projects/dynamo/inputs/deleteitem"
)

type Interface interface {
	DeleteItem(ctx context.Context, tableName string, deleteOptions ...deleteitem.OptionFunc) (*deleteitem.Result, error)
	DescribeTable(ctx context.Context, tableName string) (*describetable.Result, error)
	GetItem(ctx context.Context, tableName string, getOptions ...getitem.OptionFunc) (*getitem.Result, error)
	ListTables(ctx context.Context, listTableOptions ...listtables.OptionFunc) (*listtables.Result, error)
	PutItem(ctx context.Context, tableName string, putOptions ...putitem.OptionFunc) (*putitem.Result, error)
	Query(ctx context.Context, tableName string, queryOptions ...query.OptionFunc) (*query.Result, error)
	Scan(ctx context.Context, tableName string, scanOptions ...scan.OptionFunc) (*scan.Result, error)
}
