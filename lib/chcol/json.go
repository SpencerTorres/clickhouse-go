package chcol

import "database/sql/driver"

// JSON represents a ClickHouse JSON type that can hold multiple possible types
type JSON struct {
	paths  []string
	values []any
}

// NewJSON creates a new empty JSON value
func NewJSON() *JSON {
	return &JSON{}
}

func (o *JSON) Paths() []string {
	return o.paths
}

func (o *JSON) Values() []any {
	return o.values
}

func (o *JSON) SetValueAtPath(path string, value any) {
	// TODO: validate path is valid format, else return error

	var pathIndex = -1
	for i, currentPath := range o.paths {
		if path == currentPath {
			pathIndex = i
			break
		}
	}

	if pathIndex == -1 {
		o.paths = append(o.paths, path)
		o.values = append(o.values, value)
	} else {
		o.values[pathIndex] = value
	}
}

func (o *JSON) ValueAtPath(path string) (any, bool) {
	for i, currentPath := range o.paths {
		if path == currentPath {
			return o.values[i], true
		}
	}

	return Dynamic{}, false
}

// Scan implements the sql.Scanner interface
func (v *JSON) Scan(value interface{}) error {
	return nil
}

// Value implements the driver.Valuer interface
func (o JSON) Value() (driver.Value, error) {
	return nil, nil
}
