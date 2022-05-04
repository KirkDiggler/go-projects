package dynago

import (
	"context"
	"errors"

	"github.com/KirkDiggler/go-projects/tools/dynago/internal/entities"

	"github.com/KirkDiggler/go-projects/tools/dynago/repositories"

	"github.com/KirkDiggler/go-projects/dynamo"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

const (
	minTableNameLength         = 3
	requiresConfigMsg          = "dynago.InitializeFromConfig requires a Config"
	requiresConfigClientMsg    = "dynago.Config.Client is required"
	requiresConfigTableNameMsg = "dynago.Config.TableName is required"

	requiresRegisterConfigMsg       = "dynago.RegisterRepository requires a Config"
	requiresConfigRepoNameMsg       = "dynago.RepositoryConfig.RepoName is required"
	requiresConfigPrimaryMappingMsg = "dynago.RepositoryConfig.PrimaryMapping is required"
)

type dynago struct {
	client    dynamo.Interface
	tableName string
	tableDesc *types.TableDescription
	// TODO: should we store a data prefix here
}

type Config struct {
	Client    dynamo.Interface
	TableName string
}

func InitializeFromConfig(cfg *Config) (*dynago, error) {
	if cfg == nil {
		return nil, errors.New(requiresConfigMsg)
	}

	if cfg.Client == nil {
		return nil, errors.New(requiresConfigClientMsg)
	}

	if cfg.TableName == "" || len(cfg.TableName) < minTableNameLength {
		return nil, errors.New(requiresConfigTableNameMsg)
	}

	ctx := context.Background()

	resp, err := cfg.Client.DescribeTable(ctx, cfg.TableName)
	if err != nil {
		return nil, err
	}

	return &dynago{
		client:    cfg.Client,
		tableName: cfg.TableName,
		tableDesc: resp.Table,
	}, nil
}

// TODO: verify the table keys are not compound

type RepositoryConfig struct {
	RepoName          string
	PrimaryMapping    *entities.Mapping
	SecondaryMappings []*entities.Mapping
	Entity            interface{}
}

func (d *dynago) RegisterRepository(cfg *RepositoryConfig) (repositories.Interface, error) {
	if cfg == nil {
		return nil, errors.New(requiresRegisterConfigMsg)
	}

	if cfg.RepoName == "" {
		return nil, errors.New(requiresConfigRepoNameMsg)
	}

	if cfg.PrimaryMapping == nil {
		return nil, errors.New(requiresConfigPrimaryMappingMsg)
	}

	return repositories.New(&repositories.Config{
		Name:          cfg.RepoName,
		Client:        d.client,
		TableDesc:     d.tableDesc,
		SchemaMapping: nil,
		TableMapping:  nil,
		IndexMappings: nil,
	})
}
