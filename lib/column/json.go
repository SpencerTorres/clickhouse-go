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
	"reflect"
	"time"

	"github.com/ClickHouse/ch-go/proto"
)

const SupportedJSONSerializationVersion = 0

type ColJSON struct {
	chType Type
	name   string
	rows   int

	typedPaths   []string
	typedColumns []Interface

	skipPaths []string

	dynamicPaths   []string
	dynamicColumns []ColDynamic

	maxDynamicPaths   int
	totalDynamicPaths int
}

func (c *ColJSON) parse(t Type, tz *time.Location) (_ Interface, err error) {
	c.chType = t

	// TODO: parse typed paths, skip paths, etc.
	//c.maxTypes = 0

	//return nil, &UnsupportedColumnTypeError{
	//	t: t,
	//}

	return c, nil
}

func (c *ColJSON) Name() string {
	return c.name
}

func (c *ColJSON) Type() Type {
	return c.chType
}

func (c *ColJSON) Rows() int {
	return c.rows
}

func (c *ColJSON) Row(i int, ptr bool) any {
	return nil
}

func (c *ColJSON) ScanRow(dest any, row int) error {
	//typeIndex := c.variant.discriminators[row]
	//offsetIndex := c.variant.offsets[row]
	//var value any
	//if typeIndex != NullVariantDiscriminator {
	//	value = c.variant.columns[typeIndex].Row(offsetIndex, false)
	//}
	//
	//switch v := dest.(type) {
	//case *chcol.Dynamic:
	//	vt := chcol.NewDynamic(value)
	//	*v = vt
	//case **chcol.Dynamic:
	//	vt := chcol.NewDynamic(value)
	//	**v = vt
	//default:
	//	if typeIndex == NullVariantDiscriminator {
	//		return nil
	//	}
	//
	//	if err := c.variant.columns[typeIndex].ScanRow(dest, offsetIndex); err != nil {
	//		return err
	//	}
	//}

	return nil
}

func (c *ColJSON) Append(v any) (nulls []uint8, err error) {
	//TODO implement me
	panic("implement me")
}

func (c *ColJSON) AppendRow(v any) error {
	//var forcedType Type
	//switch v.(type) {
	//case nil:
	//	c.variant.rows++
	//	c.variant.discriminators = append(c.variant.discriminators, NullVariantDiscriminator)
	//	return nil
	//case chcol.DynamicWithType:
	//	forcedType = Type(v.(chcol.DynamicWithType).Type())
	//case *chcol.DynamicWithType:
	//	forcedType = Type(v.(*chcol.DynamicWithType).Type())
	//}
	//
	//if forcedType != "" {
	//	var i int
	//	var typeName string
	//	var col Interface
	//	var ok bool
	//	// TODO: this could be pre-calculated as a map[string]int (name->index)
	//	for i, typeName = range c.typeNames {
	//		if typeName == string(forcedType) {
	//			col = c.variant.columns[i]
	//			ok = true
	//			break
	//		}
	//	}
	//
	//	if !ok {
	//		newCol, err := forcedType.Column("", nil)
	//		if err != nil {
	//			return fmt.Errorf("value %v cannot be stored in dynamic column %s %s with forced type %s: unable to append type: %w", v, c.name, c.chType, forcedType, err)
	//		}
	//
	//		c.variant.columns = append(c.variant.columns, newCol)
	//		c.typeNames = append(c.typeNames, string(forcedType))
	//		c.totalTypes++
	//		col = newCol
	//		// totalTypes is used since SharedVariant is implicitly present and offsets discriminator index by 1
	//		i = int(c.totalTypes)
	//	}
	//
	//	if err := col.AppendRow(v); err != nil {
	//		return fmt.Errorf("value %v cannot be stored in dynamic column %s %s with forced type %s: %w", v, c.name, c.chType, forcedType, err)
	//	}
	//
	//	c.variant.rows++
	//	c.variant.discriminators = append(c.variant.discriminators, uint8(i))
	//	return nil
	//}
	//
	//// If preferred type wasn't provided, try each column
	//var err error
	//for i, col := range c.variant.columns {
	//	if err = col.AppendRow(v); err == nil {
	//		c.variant.rows++
	//		c.variant.discriminators = append(c.variant.discriminators, uint8(i))
	//		return nil
	//	}
	//}

	//return fmt.Errorf("value %v cannot be stored in json column %s %s: %w", v, c.name, c.chType, err)
	return nil
}

func (c *ColJSON) encodeHeader(buffer *proto.Buffer) {
	buffer.PutUInt64(SupportedJSONSerializationVersion)
	buffer.PutUVarInt(uint64(c.maxDynamicPaths))
	buffer.PutUVarInt(uint64(c.totalDynamicPaths))

	for _, dynamicPath := range c.dynamicPaths {
		buffer.PutString(dynamicPath)
	}

	// TODO: write typed path headers (low cardinality only?)

	for _, col := range c.dynamicColumns {
		col.encodeHeader(buffer)
	}
}

func (c *ColJSON) encodeData(buffer *proto.Buffer) {
	for _, col := range c.typedColumns {
		col.Encode(buffer)
	}

	for _, col := range c.dynamicColumns {
		col.encodeData(buffer)
	}

	// TODO: shared variant goes here?
}

func (c *ColJSON) Encode(buffer *proto.Buffer) {
	c.encodeHeader(buffer)
	c.encodeData(buffer)
}

func (c *ColJSON) ScanType() reflect.Type {
	//TODO implement me
	panic("implement me")
}

func (c *ColJSON) Reset() {
	//TODO implement me
	panic("implement me")
}

func (c *ColJSON) decodeHeader(reader *proto.Reader) error {
	jsonSerializationVersion, err := reader.UInt64()
	if err != nil {
		return fmt.Errorf("failed to read json serialization version: %w", err)
	} else if jsonSerializationVersion != SupportedJSONSerializationVersion {
		return fmt.Errorf("unsupported json serialization version: %d", jsonSerializationVersion)
	}

	maxDynamicPaths, err := reader.UVarInt()
	if err != nil {
		return fmt.Errorf("failed to read max dynamic paths for json column: %w", err)
	}
	c.maxDynamicPaths = int(maxDynamicPaths)

	totalDynamicPaths, err := reader.UVarInt()
	if err != nil {
		return fmt.Errorf("failed to read total dynamic paths for json column: %w", err)
	}
	c.totalDynamicPaths = int(totalDynamicPaths)

	c.dynamicPaths = make([]string, 0, totalDynamicPaths)
	for i := 0; i < int(totalDynamicPaths); i++ {
		strLen, err := reader.StrLen()
		if err != nil {
			return fmt.Errorf("failed to read current dynamic path name length at index %d for json column: %w", i, err)
		}

		strBytes, err := reader.ReadRaw(strLen)
		if err != nil {
			return fmt.Errorf("failed to read current dynamic path name bytes at index %d for json column: %w", i, err)
		}

		c.dynamicPaths = append(c.dynamicPaths, string(strBytes))
	}

	for range c.typedPaths {
		// TODO: read typed path prefix (low cardinality only?)
	}

	c.dynamicColumns = make([]ColDynamic, 0, totalDynamicPaths)
	for _, dynamicPath := range c.dynamicPaths {
		parsedColDynamic, _ := (&ColDynamic{}).parse("Dynamic", nil)
		colDynamic := parsedColDynamic.(*ColDynamic)

		err := colDynamic.decodeHeader(reader)
		if err != nil {
			return fmt.Errorf("failed to decode dynamic header at path %s for json column: %w", dynamicPath, err)
		}

		c.dynamicColumns = append(c.dynamicColumns, *colDynamic)
	}

	return nil
}

func (c *ColJSON) decodeData(reader *proto.Reader, rows int) error {
	for i, col := range c.typedColumns {
		typedPath := c.typedPaths[i]

		err := col.Decode(reader, rows)
		if err != nil {
			return fmt.Errorf("failed to decode %s typed path %s for json column: %w", col.Type(), typedPath, err)
		}
	}

	for i, col := range c.dynamicColumns {
		dynamicPath := c.dynamicPaths[i]

		err := col.decodeData(reader, rows)
		if err != nil {
			return fmt.Errorf("failed to decode dynamic path %s for json column: %w", dynamicPath, err)
		}
	}

	return nil
}

func (c *ColJSON) Decode(reader *proto.Reader, rows int) error {
	err := c.decodeHeader(reader)
	if err != nil {
		return fmt.Errorf("failed to decode json header: %w", err)
	}

	err = c.decodeData(reader, rows)
	if err != nil {
		return fmt.Errorf("failed to decode json data: %w", err)
	}

	return nil
}
