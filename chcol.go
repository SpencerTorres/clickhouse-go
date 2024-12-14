package clickhouse

import "github.com/ClickHouse/clickhouse-go/v2/lib/chcol"

// Re-export chcol types/funcs to top level clickhouse package

type (
	Variant         = chcol.Variant
	VariantWithType = chcol.VariantWithType
	Dynamic         = chcol.Dynamic
	DynamicWithType = chcol.DynamicWithType
	JSON            = chcol.JSON
)

// NewVariant creates a new Variant with the given value
func NewVariant(v any) Variant {
	return chcol.NewVariant(v)
}

// NewVariantWithType creates a new Variant with the given value and ClickHouse type
func NewVariantWithType(v any, chType string) VariantWithType {
	return chcol.NewVariantWithType(v, chType)
}

// NewDynamic creates a new Dynamic with the given value
func NewDynamic(v any) Dynamic {
	return chcol.NewDynamic(v)
}

// NewDynamicWithType creates a new Dynamic with the given value and ClickHouse type
func NewDynamicWithType(v any, chType string) DynamicWithType {
	return chcol.NewDynamicWithType(v, chType)
}

// NewJSON creates a new empty JSON value
func NewJSON() *JSON {
	return chcol.NewJSON()
}
