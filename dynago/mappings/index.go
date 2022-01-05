package mappings

import "github.com/KirkDiggler/go-projects/tools/dynago/entities"

type Index struct {
	Name           string
	ProjectionType entities.ProjectionType
	Mapping        Interface
}
