package chcol

import "database/sql/driver"

// JSON represents a ClickHouse JSON type that can hold multiple possible types
type JSON struct {
	valuesByPath map[string]any
}

// NewJSON creates a new empty JSON value
func NewJSON() *JSON {
	return &JSON{
		valuesByPath: make(map[string]any),
	}
}

func (o *JSON) ValuesByPath() map[string]any {
	return o.valuesByPath
}

func (o *JSON) SetValueAtPath(path string, value any) {
	// TODO: validate path is valid format, else return error
	o.valuesByPath[path] = value
}

func (o *JSON) ValueAtPath(path string) (any, bool) {
	value, ok := o.valuesByPath[path]
	return value, ok
}

// Scan implements the sql.Scanner interface
func (v *JSON) Scan(value interface{}) error {
	return nil
}

// Value implements the driver.Valuer interface
func (o JSON) Value() (driver.Value, error) {
	return nil, nil
}
