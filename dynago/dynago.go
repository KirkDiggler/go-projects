package dynago

import (
	"errors"
	"fmt"

	"github.com/KirkDiggler/go-projects/dynamo"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

const minTableNameLength = 3

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

func New(cfg *Config) (*dynago, error) {
	if cfg == nil {
		return nil, errors.New("dynago.NewMapper requires Config")
	}

	if cfg.Client == nil {
		return nil, errors.New("dynago.NewMapper requires Config.Client")
	}

	if cfg.TableName == "" || len(cfg.TableName) < minTableNameLength {
		return nil, errors.New(fmt.Sprintf("dynago.NewMapper requires Config.TableName with at least %d characters", minTableNameLength))
	}

	// TODO should I calle describe table here?

	return &dynago{
		client:    cfg.Client,
		tableName: cfg.TableName,
	}, nil
}

// TODO: verify the table keys are not compound
