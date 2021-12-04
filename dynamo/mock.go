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
	"github.com/stretchr/testify/mock"
)

type Mock struct {
	mock.Mock
}

func (m *Mock) DeleteItem(ctx context.Context, tableName string, deleteOptions ...deleteitem.OptionFunc) (*deleteitem.Result, error) {
	options := deleteitem.NewOptions(deleteOptions...)
	args := m.Called(ctx, tableName, options)

	if args.Error(1) != nil {
		return nil, args.Error(1)
	}

	return args.Get(0).(*deleteitem.Result), nil
}

func (m *Mock) DescribeTable(ctx context.Context, tableName string) (*describetable.Result, error) {
	args := m.Called(ctx, tableName)

	if args.Error(1) != nil {
		return nil, args.Error(1)
	}

	return args.Get(0).(*describetable.Result), nil
}

func (m *Mock) GetItem(ctx context.Context, tableName string, getOptions ...getitem.OptionFunc) (*getitem.Result, error) {
	options := getitem.NewOptions(getOptions...)
	args := m.Called(ctx, tableName, options)

	if args.Error(1) != nil {
		return nil, args.Error(1)
	}

	return args.Get(0).(*getitem.Result), nil
}

func (m *Mock) ListTables(ctx context.Context, listTableOptions ...listtables.OptionFunc) (*listtables.Result, error) {
	options := listtables.NewOptions(listTableOptions...)
	args := m.Called(ctx, options)

	if args.Error(1) != nil {
		return nil, args.Error(1)
	}

	return args.Get(0).(*listtables.Result), nil
}

func (m *Mock) PutItem(ctx context.Context, tableName string, putOptions ...putitem.OptionFunc) (*putitem.Result, error) {
	options := putitem.NewOptions(putOptions...)
	args := m.Called(ctx, tableName, options)

	if args.Error(1) != nil {
		return nil, args.Error(1)
	}

	return args.Get(0).(*putitem.Result), nil
}

func (m *Mock) Query(ctx context.Context, tableName string, queryOptions ...query.OptionFunc) (*query.Result, error) {
	options := query.NewOptions(queryOptions...)
	args := m.Called(ctx, tableName, options)

	if args.Error(1) != nil {
		return nil, args.Error(1)
	}

	return args.Get(0).(*query.Result), nil
}

func (m *Mock) Scan(ctx context.Context, tableName string, scanOptions ...scan.OptionFunc) (*scan.Result, error) {
	options := scan.NewOptions(scanOptions...)
	args := m.Called(ctx, tableName, options)

	if args.Error(1) != nil {
		return nil, args.Error(1)
	}

	return args.Get(0).(*scan.Result), nil
}
