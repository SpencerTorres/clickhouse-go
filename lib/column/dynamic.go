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
	"github.com/ClickHouse/clickhouse-go/v2/lib/chcol"
	"reflect"
	"time"
)

const SupportedDynamicSerializationVersion = 1

type ColDynamic struct {
	chType Type
	name   string
	rows   int

	maxTypes         uint8
	currentTypes     uint8
	currentTypeNames []string

	discriminators []uint8
	offsets        []int
	lengthsByType  map[uint8]int

	columns []Interface
	index   map[string]int
}

func (c *ColDynamic) parse(t Type, tz *time.Location) (_ Interface, err error) {
	c.chType = t

	// TODO: parse maxTypes for encoding. For decoding, max types is already included in the payload
	//c.maxTypes = 0

	//return nil, &UnsupportedColumnTypeError{
	//	t: t,
	//}

	return c, nil
}

func (c *ColDynamic) Name() string {
	return c.name
}

func (c *ColDynamic) Type() Type {
	return c.chType
}

func (c *ColDynamic) Rows() int {
	return c.rows
}

func (c *ColDynamic) Row(i int, ptr bool) any {
	typeIndex := c.discriminators[i]
	if typeIndex == NullVariantDiscriminator {
		return nil
	}

	return c.columns[typeIndex].Row(c.offsets[i], ptr)
}

func (c *ColDynamic) ScanRow(dest any, row int) error {
	typeIndex := c.discriminators[row]
	offsetIndex := c.offsets[row]
	var value any
	if typeIndex != NullVariantDiscriminator {
		value = c.columns[typeIndex].Row(offsetIndex, false)
	}

	switch v := dest.(type) {
	case *chcol.Dynamic:
		vt := chcol.NewDynamic(value)
		*v = vt
	case **chcol.Dynamic:
		vt := chcol.NewDynamic(value)
		**v = vt
	default:
		if typeIndex == NullVariantDiscriminator {
			return nil
		}

		if err := c.columns[typeIndex].ScanRow(dest, offsetIndex); err != nil {
			return err
		}
	}

	return nil
}

func (c *ColDynamic) Append(v any) (nulls []uint8, err error) {
	//TODO implement me
	panic("implement me")
}

func (c *ColDynamic) AppendRow(v any) error {
	if v == nil {
		c.rows++
		c.discriminators = append(c.discriminators, NullVariantDiscriminator)
		return nil
	}

	var forcedType Type
	switch v.(type) {
	case chcol.DynamicWithType:
		forcedType = Type(v.(chcol.DynamicWithType).Type())
	case *chcol.DynamicWithType:
		forcedType = Type(v.(*chcol.DynamicWithType).Type())
	}

	if forcedType != "" {
		var i int
		var col Interface
		var ok bool
		// TODO: this could be pre-calculated as a map[string]int (name->index)
		for i, col = range c.columns {
			if col.Type() == forcedType {
				ok = true
				break
			}
		}

		if !ok {
			newCol, err := forcedType.Column("", nil)
			if err != nil {
				return fmt.Errorf("value %v cannot be stored in dynamic column %s %s with forced type %s: unable to append type: %w", v, c.name, c.chType, forcedType, err)
			}

			c.columns = append(c.columns, newCol)
			c.currentTypes++
			col = newCol
			i = int(c.currentTypes - 1)
		}

		if err := col.AppendRow(v); err != nil {
			return fmt.Errorf("value %v cannot be stored in dynamic column %s %s with forced type %s: %w", v, c.name, c.chType, forcedType, err)
		}

		c.rows++
		c.discriminators = append(c.discriminators, uint8(i))
		return nil
	}

	// If preferred type wasn't provided, try each column
	var err error
	for i, col := range c.columns {
		if err = col.AppendRow(v); err == nil {
			c.rows++
			c.discriminators = append(c.discriminators, uint8(i))
			return nil
		}
	}

	return fmt.Errorf("value %v cannot be stored in dynamic column %s %s: %w", v, c.name, c.chType, err)
}

func (c *ColDynamic) Encode(buffer *proto.Buffer) {
	buffer.PutUInt64(SupportedDynamicSerializationVersion)
	buffer.PutUVarInt(uint64(c.maxTypes))
	buffer.PutUVarInt(uint64(c.currentTypes))

	for _, col := range c.columns {
		buffer.PutString(string(col.Type()))
	}

	buffer.PutUInt64(SupportedVariantSerializationVersion)
	buffer.PutRaw(c.discriminators)

	for _, col := range c.columns {
		col.Encode(buffer)
	}
}

func (c *ColDynamic) ScanType() reflect.Type {
	//TODO implement me
	panic("implement me")
}

func (c *ColDynamic) Reset() {
	//TODO implement me
	panic("implement me")
}

func (c *ColDynamic) Decode(reader *proto.Reader, rows int) error {
	c.rows = rows
	var err error
	dynamicSerializationVersion, err := reader.UInt64()
	if err != nil {
		return fmt.Errorf("failed to read dynamic serialization version: %w", err)
	} else if dynamicSerializationVersion != SupportedDynamicSerializationVersion {
		return fmt.Errorf("unsupported dynamic serialization version: %d", dynamicSerializationVersion)
	}

	maxTypes, err := reader.UVarInt()
	if err != nil {
		return fmt.Errorf("failed to read max types for dynamic column: %w", err)
	}
	c.maxTypes = uint8(maxTypes)

	currentTypes, err := reader.UVarInt()
	if err != nil {
		return fmt.Errorf("failed to read current types for dynamic column: %w", err)
	}
	c.currentTypes = uint8(currentTypes)

	for i := uint8(0); i < c.currentTypes; i++ {
		strLen, err := reader.StrLen()
		if err != nil {
			return fmt.Errorf("failed to read current type name length at index %d for dynamic column: %w", i, err)
		}

		strBytes, err := reader.ReadRaw(strLen)
		if err != nil {
			return fmt.Errorf("failed to read current type name bytes at index %d for dynamic column: %w", i, err)
		}

		colType := string(strBytes)
		c.currentTypeNames = append(c.currentTypeNames, colType)

		col, err := Type(strBytes).Column("", nil)
		if err != nil {
			return fmt.Errorf("failed to parse dynamic column with type %s: %w", i, err)
		}
		c.columns = append(c.columns, col)
	}

	variantSerializationVersion, err := reader.UInt64()
	if err != nil {
		return fmt.Errorf("failed to read dynamic variant serialization version: %w", err)
	} else if variantSerializationVersion != SupportedVariantSerializationVersion {
		return fmt.Errorf("unsupported dynamic variant serialization version: %d", variantSerializationVersion)
	}

	c.discriminators = make([]uint8, c.rows)
	c.offsets = make([]int, c.rows)
	c.lengthsByType = make(map[uint8]int, len(c.columns))

	for i := 0; i < c.rows; i++ {
		disc, err := reader.ReadByte()
		if err != nil {
			return fmt.Errorf("failed to read dynamic discriminator at index %d: %w", i, err)
		}

		c.discriminators[i] = disc
		if c.lengthsByType[disc] == 0 {
			c.lengthsByType[disc] = 1
		} else {
			c.lengthsByType[disc]++
		}

		c.offsets[i] = c.lengthsByType[disc] - 1
	}

	for i, col := range c.columns {
		cRows := c.lengthsByType[uint8(i)]
		if err := col.Decode(reader, cRows); err != nil {
			return fmt.Errorf("failed to decode dynamic column with %s type: %w", col.Type(), err)
		}
	}

	return nil
}
