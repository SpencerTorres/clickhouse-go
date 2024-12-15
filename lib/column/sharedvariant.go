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

package column

import (
	"bytes"
	"fmt"
	"github.com/ClickHouse/ch-go/proto"
	"reflect"
	"strings"
)

const SharedVariantInvalidType = -1

// SharedVariant. Just add more sticks of RAM if this doesn't work for you.
type SharedVariant struct {
	name string
	rows int

	discriminators []int
	offsets        []int

	columns         []Interface
	columnTypeIndex map[string]int
}

// binaryTypeToStringType maps a single byte type to a string type.
// Binary types are more complex and longer than this, but we can add a proper parser later.
var binaryTypeToStringType = map[uint8]string{
	0x00: "Nothing",
	0x01: "UInt8",
	0x02: "UInt16",
	0x03: "UInt32",
	0x04: "UInt64",
	0x05: "UInt128",
	0x06: "UInt256",
	0x07: "Int8",
	0x08: "Int16",
	0x09: "Int32",
	0x0A: "Int64",
	0x0B: "Int128",
	0x0C: "Int256",
	0x0D: "Float32",
	0x0E: "Float64",
	0x0F: "Date",
	0x10: "Date32",
	0x11: "DateTime",

	0x15: "String",

	0x1D: "UUID",

	0x21: "Set",

	0x28: "IPv4",
	0x29: "IPv6",

	0x2D: "Bool",
}

func (c *SharedVariant) Name() string {
	return c.name
}

func (c *SharedVariant) Type() Type {
	return "SharedVariant"
}

func (c *SharedVariant) Rows() int {
	return c.rows
}

func (c *SharedVariant) RowType(i int) Type {
	typeIndex := c.discriminators[i]
	if typeIndex == SharedVariantInvalidType {
		return ""
	}

	return c.columns[typeIndex].Type()
}

func (c *SharedVariant) Row(i int, ptr bool) any {
	typeIndex := c.discriminators[i]
	if typeIndex == SharedVariantInvalidType {
		return nil
	}

	return c.columns[typeIndex].Row(c.offsets[i], ptr)
}

func (c *SharedVariant) ScanRow(dest any, row int) error {
	typeIndex := c.discriminators[row]
	if typeIndex == SharedVariantInvalidType {
		return nil
	}

	return c.columns[typeIndex].ScanRow(dest, row)
}

func (c *SharedVariant) Append(v any) (nulls []uint8, err error) {
	return nil, nil
}

func (c *SharedVariant) AppendRow(v any) error {
	return nil
}

// Decode takes a String-like column and converts it into a Variant-like column. Very inefficiently.
func (c *SharedVariant) Decode(reader *proto.Reader, rows int) error {
	c.rows = rows
	c.discriminators = make([]int, rows)
	c.offsets = make([]int, rows)
	rowCountByType := make(map[int]int)
	if c.columnTypeIndex == nil {
		c.columnTypeIndex = make(map[string]int)
	}

	var bufferByType []*strings.Builder

	for i := 0; i < rows; i++ {
		rowLen, err := reader.StrLen()
		if err != nil {
			return fmt.Errorf("failed to decode SharedVariant value length at row index %d: %w", i, err)
		}

		typeLength := 1
		typeByte, err := reader.ReadByte()
		if err != nil {
			return fmt.Errorf("failed to decode SharedVariant value at row index %d: %w", i, err)
		}

		typeName, ok := binaryTypeToStringType[typeByte]
		if !ok {
			c.discriminators[i] = SharedVariantInvalidType
			// Unknown type, skip buffer to next row
			_, err := reader.ReadRaw(rowLen - typeLength)
			if err != nil {
				return fmt.Errorf("failed to skip in SharedVariant: %w", err)
			}

			continue
		}

		colIndex, ok := c.columnTypeIndex[typeName]
		if !ok {
			newCol, err := Type(typeName).Column("", nil)
			if err != nil {
				return fmt.Errorf("failed to add column of type \"%s\" to SharedVariant: %w", typeName, err)
			}

			c.columns = append(c.columns, newCol)
			colIndex = len(c.columns) - 1
			c.columnTypeIndex[typeName] = colIndex

			bufferByType = append(bufferByType, &strings.Builder{})
		}

		rowData, err := reader.ReadRaw(rowLen - typeLength)
		if err != nil {
			return fmt.Errorf("failed to copy row data for column of type \"%s\" to SharedVariant: %w", typeName, err)
		}
		bufferByType[colIndex].Write(rowData)

		c.discriminators[i] = colIndex
		if rowCountByType[colIndex] == 0 {
			rowCountByType[colIndex] = 1
		} else {
			rowCountByType[colIndex]++
		}

		c.offsets[i] = rowCountByType[colIndex] - 1
	}

	for colIndex, rowData := range bufferByType {
		rowCount := rowCountByType[colIndex]
		col := c.columns[colIndex]

		rowReader := proto.NewReader(bytes.NewReader([]byte(rowData.String())))
		err := col.Decode(rowReader, rowCount)
		if err != nil {
			return fmt.Errorf("failed to decode row for column of type \"%s\" to SharedVariant: %w", col.Type(), err)
		}
	}

	return nil
}

func (c *SharedVariant) Encode(buffer *proto.Buffer) {
}

func (c *SharedVariant) ScanType() reflect.Type {
	return nil
}

func (c *SharedVariant) Reset() {
}
