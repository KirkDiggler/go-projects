package repositories

import (
	"context"

	"github.com/KirkDiggler/go-projects/tools/dynago/repositories/options/putitem"
)

type Interface interface {
	Put(context.Context, ...func(*putitem.Options)) (*putitem.Result, error)
}
