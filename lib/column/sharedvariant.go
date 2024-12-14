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
	"fmt"
	"github.com/ClickHouse/ch-go/proto"
	"reflect"
)

const SharedVariantInvalidType = -1

type SharedVariant struct {
	name string
	rows int

	discriminators []int
	offsets        []int

	columns         []Interface
	columnTypeIndex map[string]int

	// Just add more sticks of RAM if this doesn't work for you
	rowColumns []Interface
}

var binaryTypeToStringType = map[uint8]string{
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

func (c *SharedVariant) Row(i int, ptr bool) any {
	return c.rowColumns[i].Row(0, ptr)
}

func (c *SharedVariant) ScanRow(dest any, row int) error {
	return c.rowColumns[row].ScanRow(dest, 0)
}

func (c *SharedVariant) Append(v any) (nulls []uint8, err error) {
	return nil, nil
}

func (c *SharedVariant) AppendRow(v any) error {
	return nil
}

func (c *SharedVariant) Decode(reader *proto.Reader, rows int) error {
	c.rows = rows

	for i := 0; i < rows; i++ {
		_, err := reader.StrLen()
		if err != nil {
			return fmt.Errorf("failed to decode SharedVariant value length at row index %d: %w", i, err)
		}

		typeByte, err := reader.ReadByte()
		if err != nil {
			return fmt.Errorf("failed to decode SharedVariant value at row index %d: %w", i, err)
		}

		typeName, ok := binaryTypeToStringType[typeByte]
		if !ok {
			continue
		}

		col, err := Type(typeName).Column("", nil)
		if err != nil {
			return fmt.Errorf("failed to add column of type \"%s\" to SharedVariant at row index %d: %w", typeName, i, err)
		}

		err = col.Decode(reader, 1)
		if err != nil {
			return fmt.Errorf("failed to decode row for column of type \"%s\" to SharedVariant: %w", typeName, err)
		}

		c.rowColumns = append(c.rowColumns, col)
	}

	return nil
}

func (c *SharedVariant) OptimalDecode(reader *proto.Reader, rows int) error {
	c.rows = rows
	c.discriminators = make([]int, rows)
	c.offsets = make([]int, rows)
	rowCountByType := make(map[int]int)
	if c.columnTypeIndex == nil {
		c.columnTypeIndex = make(map[string]int)
	}

	for i := 0; i < rows; i++ {
		strLen, err := reader.StrLen()
		if err != nil {
			return fmt.Errorf("failed to decode SharedVariant value length at row index %d: %w", i, err)
		}

		typeByte, err := reader.ReadByte()
		if err != nil {
			return fmt.Errorf("failed to decode SharedVariant value at row index %d: %w", i, err)
		}

		typeName, ok := binaryTypeToStringType[typeByte]
		if !ok {
			c.discriminators[i] = SharedVariantInvalidType
			reader.ReadRaw(strLen - 1)
			continue
		}

		var col Interface
		colIndex, ok := c.columnTypeIndex[typeName]
		if ok {
			col = c.columns[colIndex]
		} else {
			newCol, err := Type(typeName).Column("", nil)
			if err != nil {
				return fmt.Errorf("failed to add column of type \"%s\" to SharedVariant: %w", typeName, err)
			}

			c.columns = append(c.columns, newCol)
			col = newCol
			colIndex = len(c.columns) - 1
			c.columnTypeIndex[typeName] = colIndex
		}

		err = col.Decode(reader, 1)
		if err != nil {
			return fmt.Errorf("failed to decode row for column of type \"%s\" to SharedVariant: %w", typeName, err)
		}

		c.discriminators[i] = colIndex
		if rowCountByType[colIndex] == 0 {
			rowCountByType[colIndex] = 1
		} else {
			rowCountByType[colIndex]++
		}

		c.offsets[i] = rowCountByType[colIndex] - 1
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
