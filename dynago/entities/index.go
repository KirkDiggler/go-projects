package entities

type ProjectionType string

const (
	PropjectionTypeAll      ProjectionType = "ALL"
	PropjectionTypeKeysOnly ProjectionType = "KEYS_ONLY"
)

type Index struct {
	Name           string // Gets set when loading from entity TODO: find a better way to keep this internal
	ProjectionType ProjectionType
	Mapping        *Mapping
}
