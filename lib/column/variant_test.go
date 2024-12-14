package column

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestColVariant_parse(t *testing.T) {
	cases := []struct {
		typ           Type
		expectedTypes []Type
	}{
		{typ: "Variant(Int64)", expectedTypes: []Type{"Int64"}},
		{typ: "Variant(Int64, String)", expectedTypes: []Type{"Int64", "String"}},
		{typ: "Variant(Array(String), Int64, String)", expectedTypes: []Type{"Array(String)", "Int64", "String"}},
		{typ: "Variant(Array(Map(String, String)), Map(String, Int64))", expectedTypes: []Type{"Array(Map(String, String))", "Map(String, Int64)"}},
		{typ: "Variant(Array(Map(String, Tuple(a String, b Int64))), Map(String, Int64))", expectedTypes: []Type{"Array(Map(String, Tuple(a String, b Int64)))", "Map(String, Int64)"}},
	}

	for i, c := range cases {
		col, err := (&ColVariant{name: "vt"}).parse(c.typ, nil)
		if err != nil {
			t.Fatalf("case index %d failed to parse Variant column: %s", i, err)
		}

		require.Equal(t, "vt", col.Name())
		require.Equal(t, c.typ, col.chType)
		require.Equal(t, len(c.expectedTypes), len(col.columns))
		require.Equal(t, len(c.expectedTypes), len(col.columnTypeIndex))

		for j, subCol := range col.columns {
			expectedType := c.expectedTypes[j]
			actualType := subCol.Type()
			if actualType != expectedType {
				t.Fatalf("case index %d Variant type index %d column type does not match: expected: \"%s\" actual: \"%s\"", i, j, expectedType, actualType)
			}

			expectedColumnTypeIndex := uint8(j)
			actualColumnTypeIndex := col.columnTypeIndex[string(actualType)]
			if actualColumnTypeIndex != expectedColumnTypeIndex {
				t.Fatalf("case index %d Variant type index %d columnTypeIndex does not match: expected: %d actual: %d", i, j, expectedColumnTypeIndex, actualColumnTypeIndex)
			}
		}
	}
}

func TestColVariant_parse_invalid(t *testing.T) {
	cases := []Type{
		"",
		"Variant",
		"Variant(Array(Map(String)), Map(String, Int64))",
		"Variant(Array(Tuple(String, b Int64)), Map(String, Int64), FakeType)",
	}

	for i, typeName := range cases {
		_, err := (&ColVariant{name: "vt"}).parse(typeName, nil)
		if err == nil {
			t.Fatalf("expected error for case index %d (\"%s\"), but received nil", i, typeName)
		}
	}
}

func TestColVariant_addColumn(t *testing.T) {
	col := ColVariant{columnTypeIndex: make(map[string]uint8, 1)}

	col.addColumn(&Int64{})

	require.Equal(t, 1, len(col.columns))
	require.Equal(t, 1, len(col.columnTypeIndex))
	require.Equal(t, Type("Int64"), col.columns[0].Type())
	require.Equal(t, uint8(0), col.columnTypeIndex["Int64"])
}

func TestColVariant_appendDiscriminatorRow(t *testing.T) {
	col := ColVariant{}
	var discriminator uint8 = 8

	col.appendDiscriminatorRow(discriminator)

	require.Equal(t, 1, len(col.discriminators))
	require.Equal(t, discriminator, col.discriminators[0])
	require.Equal(t, 1, col.rows)
}

func TestColVariant_appendNullRow(t *testing.T) {
	col := ColVariant{}

	col.appendNullRow()

	require.Equal(t, 1, len(col.discriminators))
	require.Equal(t, NullVariantDiscriminator, col.discriminators[0])
	require.Equal(t, 1, col.rows)
}
