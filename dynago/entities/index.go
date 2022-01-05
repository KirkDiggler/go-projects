package entities

type ProjectionType string

const (
	PropjectionTypeAll      ProjectionType = "ALL"
	PropjectionTypeKeysOnly ProjectionType = "KEYS_ONLY"
)

type Index struct {
	Name           string
	ProjectionType ProjectionType
	Mapping        *Mapping
}
