// Licensed to ClickHouse, Inc. under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership. ClickHouse, Inc. licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package binary

import (
	"fmt"
	"github.com/ClickHouse/ch-go/proto"
	"strconv"
	"strings"
)

// ReadBinaryType will recursively read a binary type and convert it to a string type.
// Returns type name, bytes read, and "ok" bool
// Binary type docs: https://clickhouse.com/docs/en/sql-reference/data-types/data-types-binary-encoding
func ReadBinaryType(reader *proto.Reader) (string, int, bool) {
	var tLen int
	t, err := reader.ReadByte()
	if err != nil {
		return "", tLen, false
	}
	tLen += 1

	switch t {
	case 0x00:
		return "Nothing", tLen, true
	case 0x01:
		return "UInt8", tLen, true
	case 0x02:
		return "UInt16", tLen, true
	case 0x03:
		return "UInt32", tLen, true
	case 0x04:
		return "UInt64", tLen, true
	case 0x05:
		return "UInt128", tLen, true
	case 0x06:
		return "UInt256", tLen, true
	case 0x07:
		return "Int8", tLen, true
	case 0x08:
		return "Int16", tLen, true
	case 0x09:
		return "Int32", tLen, true
	case 0x0A:
		return "Int64", tLen, true
	case 0x0B:
		return "Int128", tLen, true
	case 0x0C:
		return "Int256", tLen, true
	case 0x0D:
		return "Float32", tLen, true
	case 0x0E:
		return "Float64", tLen, true
	case 0x0F:
		return "Date", tLen, true
	case 0x10:
		return "Date32", tLen, true
	case 0x11:
		return "DateTime", tLen, true
	case 0x12:
		timeZoneNameSize, err := reader.UVarInt()
		if err != nil {
			return "", tLen, false
		}
		tLen += uVarIntByteLength(timeZoneNameSize)

		timeZoneNameData, err := reader.ReadRaw(int(timeZoneNameSize))
		if err != nil {
			return "", tLen, false
		}
		tLen += int(timeZoneNameSize)

		return fmt.Sprintf("DateTime('%s')", string(timeZoneNameData)), tLen, true
	case 0x13:
		precision, err := reader.ReadByte()
		if err != nil {
			return "", tLen, false
		}
		tLen += 1

		return fmt.Sprintf("DateTime64(%d)", precision), tLen, true
	case 0x14:
		precision, err := reader.ReadByte()
		if err != nil {
			return "", tLen, false
		}
		tLen += 1

		timeZoneNameSize, err := reader.UVarInt()
		if err != nil {
			return "", tLen, false
		}
		tLen += uVarIntByteLength(timeZoneNameSize)

		timeZoneNameData, err := reader.ReadRaw(int(timeZoneNameSize))
		if err != nil {
			return "", tLen, false
		}
		tLen += int(timeZoneNameSize)

		return fmt.Sprintf("DateTime64(%d, '%s')", precision, string(timeZoneNameData)), tLen, true
	case 0x15:
		return "String", tLen, true
	case 0x16:
		precision, err := reader.ReadByte()
		if err != nil {
			return "", tLen, false
		}
		tLen += 1

		return fmt.Sprintf("FixedString(%d)", precision), tLen, true
	case 0x17:
		elementCount, err := reader.UVarInt()
		if err != nil {
			return "", tLen, false
		}
		tLen += uVarIntByteLength(elementCount)

		var enumTypeName strings.Builder
		enumTypeName.WriteString("Enum8(")

		for i := 0; i < int(elementCount); i++ {
			elementNameSize, err := reader.UVarInt()
			if err != nil {
				return "", tLen, false
			}
			tLen += uVarIntByteLength(elementNameSize)

			elementNameData, err := reader.ReadRaw(int(elementNameSize))
			if err != nil {
				return "", tLen, false
			}
			tLen += int(elementNameSize)

			elementValue, err := reader.ReadByte()
			if err != nil {
				return "", tLen, false
			}
			tLen += 1

			enumTypeName.WriteByte('\'')
			enumTypeName.Write(elementNameData)
			enumTypeName.WriteByte('\'')
			enumTypeName.WriteString(" = ")
			enumTypeName.WriteString(strconv.Itoa(int(elementValue)))
			if i != int(elementCount)-1 {
				enumTypeName.WriteString(", ")
			}
		}

		enumTypeName.WriteString(")")
		return enumTypeName.String(), tLen, true
	case 0x18:
		elementCount, err := reader.UVarInt()
		if err != nil {
			return "", tLen, false
		}
		tLen += uVarIntByteLength(elementCount)

		var enumTypeName strings.Builder
		enumTypeName.WriteString("Enum16(")

		for i := 0; i < int(elementCount); i++ {
			elementNameSize, err := reader.UVarInt()
			if err != nil {
				return "", tLen, false
			}
			tLen += uVarIntByteLength(elementNameSize)

			elementNameData, err := reader.ReadRaw(int(elementNameSize))
			if err != nil {
				return "", tLen, false
			}
			tLen += int(elementNameSize)

			elementValueBytes, err := reader.ReadRaw(2)
			if err != nil {
				return "", tLen, false
			}
			tLen += 1

			elementValue := uint16(elementValueBytes[0]) | (uint16(elementValueBytes[1]) << 8)

			enumTypeName.WriteByte('\'')
			enumTypeName.Write(elementNameData)
			enumTypeName.WriteByte('\'')
			enumTypeName.WriteString(" = ")
			enumTypeName.WriteString(strconv.Itoa(int(elementValue)))
			if i != int(elementCount)-1 {
				enumTypeName.WriteString(", ")
			}
		}

		enumTypeName.WriteString(")")
		return enumTypeName.String(), tLen, true
	case 0x19:
		precision, err := reader.ReadByte()
		if err != nil {
			return "", tLen, false
		}
		tLen += 1

		scale, err := reader.ReadByte()
		if err != nil {
			return "", tLen, false
		}
		tLen += 1

		return fmt.Sprintf("Decimal32(%d, %d)", precision, scale), tLen, true
	case 0x1A:
		precision, err := reader.ReadByte()
		if err != nil {
			return "", tLen, false
		}
		tLen += 1

		scale, err := reader.ReadByte()
		if err != nil {
			return "", tLen, false
		}
		tLen += 1

		return fmt.Sprintf("Decimal64(%d, %d)", precision, scale), tLen, true
	case 0x1B:
		precision, err := reader.ReadByte()
		if err != nil {
			return "", tLen, false
		}
		tLen += 1

		scale, err := reader.ReadByte()
		if err != nil {
			return "", tLen, false
		}
		tLen += 1

		return fmt.Sprintf("Decimal128(%d, %d)", precision, scale), tLen, true
	case 0x1C:
		precision, err := reader.ReadByte()
		if err != nil {
			return "", tLen, false
		}
		tLen += 1

		scale, err := reader.ReadByte()
		if err != nil {
			return "", tLen, false
		}
		tLen += 1

		return fmt.Sprintf("Decimal256(%d, %d)", precision, scale), tLen, true
	case 0x1D:
		return "UUID", tLen, true
	case 0x1E:
		subTypeName, subTypeLen, ok := ReadBinaryType(reader)
		if !ok {
			return "", tLen, false
		}
		tLen += subTypeLen

		return fmt.Sprintf("Array(%s)", subTypeName), tLen, true
	case 0x1F:
		elementCount, err := reader.UVarInt()
		if err != nil {
			return "", tLen, false
		}
		tLen += uVarIntByteLength(elementCount)

		var tupleTypeName strings.Builder
		tupleTypeName.WriteString("Tuple(")

		for i := 0; i < int(elementCount); i++ {
			subTypeName, subTypeLen, ok := ReadBinaryType(reader)
			if !ok {
				return "", tLen, false
			}
			tLen += subTypeLen

			tupleTypeName.WriteString(subTypeName)
			if i != int(elementCount)-1 {
				tupleTypeName.WriteString(", ")
			}
		}

		tupleTypeName.WriteByte(')')

		return tupleTypeName.String(), tLen, true
	case 0x20:
		elementCount, err := reader.UVarInt()
		if err != nil {
			return "", tLen, false
		}
		tLen += uVarIntByteLength(elementCount)

		var tupleTypeName strings.Builder
		tupleTypeName.WriteString("Tuple(")

		for i := 0; i < int(elementCount); i++ {
			elementNameSize, err := reader.UVarInt()
			if err != nil {
				return "", tLen, false
			}
			tLen += uVarIntByteLength(elementNameSize)

			elementNameData, err := reader.ReadRaw(int(elementNameSize))
			if err != nil {
				return "", tLen, false
			}
			tLen += int(elementNameSize)

			tupleTypeName.Write(elementNameData)
			tupleTypeName.WriteByte(' ')

			subTypeName, subTypeLen, ok := ReadBinaryType(reader)
			if !ok {
				return "", tLen, false
			}
			tLen += subTypeLen

			tupleTypeName.WriteString(subTypeName)
			if i != int(elementCount)-1 {
				tupleTypeName.WriteString(", ")
			}
		}

		tupleTypeName.WriteByte(')')
		return tupleTypeName.String(), tLen, true
	case 0x21:
		return "Set", tLen, true
	case 0x22:
		// Interval
		return "", tLen, false
	case 0x23:
		subTypeName, subTypeLen, ok := ReadBinaryType(reader)
		if !ok {
			return "", tLen, false
		}
		tLen += subTypeLen

		return fmt.Sprintf("Nullable(%s)", subTypeName), tLen, true
	case 0x24:
		// Function
		return "", tLen, false
	case 0x25:
		// AggregateFunction
		return "", tLen, false
	case 0x26:
		subTypeName, subTypeLen, ok := ReadBinaryType(reader)
		if !ok {
			return "", tLen, false
		}
		tLen += subTypeLen

		return fmt.Sprintf("LowCardinality(%s)", subTypeName), tLen, true
	case 0x27:
		keyTypeName, keyTypeLen, ok := ReadBinaryType(reader)
		if !ok {
			return "", tLen, false
		}
		tLen += keyTypeLen

		valueTypeName, valueTypeLen, ok := ReadBinaryType(reader)
		if !ok {
			return "", tLen, false
		}
		tLen += valueTypeLen

		return fmt.Sprintf("Map(%s, %s)", keyTypeName, valueTypeName), tLen, true
	case 0x28:
		return "IPv4", tLen, true
	case 0x29:
		return "IPv6", tLen, true
	case 0x2A:
		variantCount, err := reader.UVarInt()
		if err != nil {
			return "", tLen, false
		}
		tLen += uVarIntByteLength(variantCount)

		var variantTypeName strings.Builder
		variantTypeName.WriteString("Variant(")

		for i := 0; i < int(variantCount); i++ {
			subTypeName, subTypeLen, ok := ReadBinaryType(reader)
			if !ok {
				return "", tLen, false
			}
			tLen += subTypeLen

			variantTypeName.WriteString(subTypeName)
			if i != int(variantCount)-1 {
				variantTypeName.WriteString(", ")
			}
		}

		variantTypeName.WriteByte(')')
		return variantTypeName.String(), tLen, true
	case 0x2B:
		maxTypes, err := reader.ReadByte()
		if err != nil {
			return "", tLen, false
		}
		tLen += 1

		return fmt.Sprintf("Dynamic(max_types=%d)", maxTypes), tLen, true
	case 0x2C:
		customTypeNameSize, err := reader.UVarInt()
		if err != nil {
			return "", tLen, false
		}
		tLen += uVarIntByteLength(customTypeNameSize)

		customTypeNameData, err := reader.ReadRaw(int(customTypeNameSize))
		if err != nil {
			return "", tLen, false
		}
		tLen += int(customTypeNameSize)

		return string(customTypeNameData), tLen, false
	case 0x2D:
		return "Bool", tLen, true
	case 0x2E:
		// SimpleAggregateFunction
		return "", tLen, false
	case 0x2F:
		elementCount, err := reader.UVarInt()
		if err != nil {
			return "", tLen, false
		}
		tLen += uVarIntByteLength(elementCount)

		var nestedTypeName strings.Builder
		nestedTypeName.WriteString("Nested(")

		for i := 0; i < int(elementCount); i++ {
			elementNameSize, err := reader.UVarInt()
			if err != nil {
				return "", tLen, false
			}
			tLen += uVarIntByteLength(elementNameSize)

			elementNameData, err := reader.ReadRaw(int(elementNameSize))
			if err != nil {
				return "", tLen, false
			}
			tLen += int(elementNameSize)

			nestedTypeName.Write(elementNameData)
			nestedTypeName.WriteByte(' ')

			subTypeName, subTypeLen, ok := ReadBinaryType(reader)
			if !ok {
				return "", tLen, false
			}
			tLen += subTypeLen

			nestedTypeName.WriteString(subTypeName)
			if i != int(elementCount)-1 {
				nestedTypeName.WriteString(", ")
			}
		}

		nestedTypeName.WriteByte(')')
		return nestedTypeName.String(), tLen, true
	case 0x30:
		var jsonTypeName strings.Builder

		// serialization version (ignored)
		_, err := reader.ReadByte()
		if err != nil {
			return "", tLen, false
		}
		tLen += 1

		maxDynamicPaths, err := reader.UVarInt()
		if err != nil {
			return "", tLen, false
		}
		tLen += uVarIntByteLength(maxDynamicPaths)

		maxDynamicTypes, err := reader.ReadByte()
		if err != nil {
			return "", tLen, false
		}
		tLen += 1

		typedPathCount, err := reader.UVarInt()
		if err != nil {
			return "", tLen, false
		}
		tLen += uVarIntByteLength(typedPathCount)

		typedPathNames := make([]string, 0, typedPathCount)
		typedPathTypes := make([]string, 0, typedPathCount)
		for i := 0; i < int(typedPathCount); i++ {
			typedPathNameSize, err := reader.UVarInt()
			if err != nil {
				return "", tLen, false
			}
			tLen += uVarIntByteLength(typedPathNameSize)

			typedPathNameData, err := reader.ReadRaw(int(typedPathNameSize))
			if err != nil {
				return "", tLen, false
			}
			tLen += int(typedPathNameSize)

			typedPathType, typedPathTypeLen, ok := ReadBinaryType(reader)
			if !ok {
				return "", tLen, false
			}
			tLen += typedPathTypeLen

			typedPathNames = append(typedPathNames, string(typedPathNameData))
			typedPathTypes = append(typedPathTypes, string(typedPathType))
		}

		skipPathCount, err := reader.UVarInt()
		if err != nil {
			return "", tLen, false
		}
		tLen += uVarIntByteLength(skipPathCount)

		skipPathNames := make([]string, 0, skipPathCount)
		for i := 0; i < int(skipPathCount); i++ {
			skipPathNameSize, err := reader.UVarInt()
			if err != nil {
				return "", tLen, false
			}
			tLen += uVarIntByteLength(skipPathNameSize)

			skipPathNameData, err := reader.ReadRaw(int(skipPathNameSize))
			if err != nil {
				return "", tLen, false
			}
			tLen += int(skipPathNameSize)

			skipPathNames = append(skipPathNames, string(skipPathNameData))
		}

		skipPathRegexpCount, err := reader.UVarInt()
		if err != nil {
			return "", tLen, false
		}
		tLen += uVarIntByteLength(skipPathRegexpCount)

		skipPathRegexpNames := make([]string, 0, skipPathRegexpCount)
		for i := 0; i < int(skipPathRegexpCount); i++ {
			skipPathRegexpNameSize, err := reader.UVarInt()
			if err != nil {
				return "", tLen, false
			}
			tLen += uVarIntByteLength(skipPathRegexpNameSize)

			skipPathRegexpNameData, err := reader.ReadRaw(int(skipPathRegexpNameSize))
			if err != nil {
				return "", tLen, false
			}
			tLen += int(skipPathRegexpNameSize)

			skipPathRegexpNames = append(skipPathRegexpNames, string(skipPathRegexpNameData))
		}

		jsonTypeName.WriteString("JSON(")
		jsonTypeName.WriteString("max_dynamic_paths=")
		jsonTypeName.WriteString(strconv.Itoa(int(maxDynamicPaths)))
		jsonTypeName.WriteString(", ")
		jsonTypeName.WriteString("max_dynamic_types=")
		jsonTypeName.WriteString(strconv.Itoa(int(maxDynamicTypes)))

		if len(typedPathNames) > 0 || len(skipPathNames) > 0 || len(skipPathRegexpNames) > 0 {
			jsonTypeName.WriteString(", ")
		}

		for i, typedPathName := range typedPathNames {
			typedPathType := typedPathTypes[i]

			jsonTypeName.WriteString(typedPathName)
			jsonTypeName.WriteByte(' ')
			jsonTypeName.WriteString(typedPathType)
			if i != len(typedPathNames)-1 {
				jsonTypeName.WriteString(", ")
			}
		}

		for i, skipPathName := range skipPathNames {
			jsonTypeName.WriteString("SKIP ")
			jsonTypeName.WriteString(skipPathName)
			jsonTypeName.WriteByte(' ')
			if i != len(skipPathNames)-1 {
				jsonTypeName.WriteString(", ")
			}
		}

		for i, skipPathRegexpName := range skipPathRegexpNames {
			jsonTypeName.WriteString("SKIP REGEXP")
			jsonTypeName.WriteByte(' ')
			jsonTypeName.WriteByte('\'')
			jsonTypeName.WriteString(skipPathRegexpName)
			jsonTypeName.WriteByte('\'')
			jsonTypeName.WriteByte(' ')
			if i != len(skipPathRegexpNames)-1 {
				jsonTypeName.WriteString(", ")
			}
		}

		return jsonTypeName.String(), tLen, true
	default:
		return "", tLen, false
	}
}

// uVarIntByteLength returns the number of bytes required to store a UVarInt of a specific value
func uVarIntByteLength(x uint64) int {
	size := 1
	for x >= 0x80 {
		x >>= 7
		size++
	}

	return size
}
