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
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/ClickHouse/ch-go/proto"
	"github.com/ClickHouse/clickhouse-go/v2/lib/chcol"
)

const SupportedDynamicSerializationVersion = 1
const DefaultMaxDynamicTypes = 32

type ColDynamic struct {
	chType Type
	tz     *time.Location

	name string

	maxTypes       uint8
	totalTypes     uint8
	typeNames      []string
	typeNamesIndex map[string]int

	variant ColVariant
}

func (c *ColDynamic) parse(t Type, tz *time.Location) (_ Interface, err error) {
	c.chType = t
	c.tz = tz
	tStr := string(t)

	// SharedVariant is special, and does not count against totalTypes
	c.typeNamesIndex = make(map[string]int)
	c.addTypeName("SharedVariant")
	sv, _ := Type("String").Column("", tz)
	c.variant.columnTypeIndex = make(map[string]uint8)
	c.variant.addColumn(sv)

	c.maxTypes = DefaultMaxDynamicTypes
	c.totalTypes = 0

	if tStr == "Dynamic" {
		return c, nil
	}

	if !strings.HasPrefix(tStr, "Dynamic(") || !strings.HasSuffix(tStr, ")") {
		return nil, &UnsupportedColumnTypeError{t: t}
	}

	typeParamsStr := strings.TrimPrefix(tStr, "Dynamic(")
	typeParamsStr = strings.TrimSuffix(typeParamsStr, ")")

	if strings.HasPrefix(typeParamsStr, "max_types=") {
		v := strings.TrimPrefix(typeParamsStr, "max_types=")
		if maxTypes, err := strconv.Atoi(v); err == nil {
			c.maxTypes = uint8(maxTypes)
		}
	}

	return c, nil
}

func (c *ColDynamic) addTypeName(typeName string) {
	c.typeNames = append(c.typeNames, typeName)
	c.typeNamesIndex[typeName] = len(c.typeNames) - 1
	c.totalTypes++
}

func (c *ColDynamic) Name() string {
	return c.name
}

func (c *ColDynamic) Type() Type {
	return c.chType
}

func (c *ColDynamic) Rows() int {
	return c.variant.rows
}

func (c *ColDynamic) Row(i int, ptr bool) any {
	typeIndex := c.variant.discriminators[i]
	if typeIndex == NullVariantDiscriminator {
		return nil
	}

	return c.variant.columns[typeIndex].Row(c.variant.offsets[i], ptr)
}

func (c *ColDynamic) ScanRow(dest any, row int) error {
	typeIndex := c.variant.discriminators[row]
	offsetIndex := c.variant.offsets[row]
	var value any
	if typeIndex != NullVariantDiscriminator {
		value = c.variant.columns[typeIndex].Row(offsetIndex, false)
	}

	switch v := dest.(type) {
	case *chcol.Dynamic:
		vt := chcol.NewDynamic(value)
		*v = vt
	case **chcol.Dynamic:
		vt := chcol.NewDynamic(value)
		**v = vt
	case *chcol.DynamicWithType:
		vt := chcol.NewDynamicWithType(value, string(c.variant.columns[typeIndex].Type()))
		*v = vt
	case **chcol.DynamicWithType:
		vt := chcol.NewDynamicWithType(value, string(c.variant.columns[typeIndex].Type()))
		**v = vt
	default:
		if typeIndex == NullVariantDiscriminator {
			return nil
		}

		if err := c.variant.columns[typeIndex].ScanRow(dest, offsetIndex); err != nil {
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
	var requestedType string
	switch v.(type) {
	case nil:
		c.variant.rows++
		c.variant.discriminators = append(c.variant.discriminators, NullVariantDiscriminator)
		return nil
	case chcol.DynamicWithType:
		requestedType = v.(chcol.DynamicWithType).Type()
	case *chcol.DynamicWithType:
		requestedType = v.(*chcol.DynamicWithType).Type()
	}

	if requestedType != "" {
		var col Interface
		colIndex, ok := c.typeNamesIndex[requestedType]
		if ok {
			col = c.variant.columns[colIndex]
		} else {
			newCol, err := Type(requestedType).Column("", c.tz)
			if err != nil {
				return fmt.Errorf("value \"%v\" cannot be stored in dynamic column %s with requested type %s: unable to append type: %w", v, c.chType, requestedType, err)
			}

			c.addTypeName(requestedType)
			colIndex = int(c.totalTypes)
			c.variant.addColumn(newCol)
			col = newCol
		}

		if err := col.AppendRow(v); err != nil {
			return fmt.Errorf("value \"%v\" cannot be stored in dynamic column %s with requested type %s: %w", v, c.chType, requestedType, err)
		}

		c.variant.rows++
		c.variant.discriminators = append(c.variant.discriminators, uint8(colIndex))
		return nil
	}

	// If preferred type wasn't provided, try each column
	for i, col := range c.variant.columns {
		if c.typeNames[i] == "SharedVariant" {
			continue
		}

		if err := col.AppendRow(v); err == nil {
			c.variant.rows++
			c.variant.discriminators = append(c.variant.discriminators, uint8(i))
			return nil
		}
	}

	// If no existing columns match, try matching a ClickHouse type from common Go types
	inferredTypeName := inferClickHouseTypeFromGoType(v)
	if inferredTypeName != "" {
		return c.AppendRow(chcol.NewDynamicWithType(v, inferredTypeName))
	}

	return fmt.Errorf("value \"%v\" cannot be stored in dynamic column: no compatible types. hint: use %s to wrap the value", v, scanTypeDynamic.String())
}

func (c *ColDynamic) sortColumnsForEncoding() {
	sortedTypeNames := make([]string, 0, len(c.typeNames))
	sortedTypeNames = append(sortedTypeNames, c.typeNames...)
	slices.Sort(sortedTypeNames)

	sortedIndices := make(map[string]int, len(c.typeNames))
	for i, typeName := range sortedTypeNames {
		sortedIndices[typeName] = i
	}

	nextDiscriminators := make([]uint8, len(c.variant.columns))
	nextColumns := make([]Interface, len(c.variant.columns))
	for i, typeName := range c.typeNames {
		correctIndex := sortedIndices[typeName]
		nextDiscriminators[i] = uint8(correctIndex)
		nextColumns[correctIndex] = c.variant.columns[i]
		c.typeNamesIndex[typeName] = correctIndex
		c.variant.columnTypeIndex[typeName] = uint8(correctIndex)
	}

	for i := range c.variant.discriminators {
		if c.variant.discriminators[i] == NullVariantDiscriminator {
			continue
		}

		c.variant.discriminators[i] = nextDiscriminators[c.variant.discriminators[i]]
	}

	c.variant.columns = nextColumns
	c.typeNames = sortedTypeNames
}

func (c *ColDynamic) encodeHeader(buffer *proto.Buffer) {
	c.sortColumnsForEncoding()

	buffer.PutUInt64(SupportedDynamicSerializationVersion)
	buffer.PutUVarInt(uint64(c.maxTypes))
	buffer.PutUVarInt(uint64(c.totalTypes))

	for _, typeName := range c.typeNames {
		if typeName == "SharedVariant" {
			continue
		}

		buffer.PutString(typeName)
	}

	c.variant.encodeHeader(buffer)
}

func (c *ColDynamic) encodeData(buffer *proto.Buffer) {
	c.variant.encodeData(buffer)
}

func (c *ColDynamic) Encode(buffer *proto.Buffer) {
	c.encodeHeader(buffer)
	c.encodeData(buffer)
}

func (c *ColDynamic) ScanType() reflect.Type {
	return scanTypeDynamic
}

func (c *ColDynamic) Reset() {
	//TODO implement me
	panic("implement me")
}

func (c *ColDynamic) decodeHeader(reader *proto.Reader) error {
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

	totalTypes, err := reader.UVarInt()
	if err != nil {
		return fmt.Errorf("failed to read total types for dynamic column: %w", err)
	}
	c.totalTypes = uint8(totalTypes)

	c.typeNames = make([]string, 0, totalTypes+1)
	c.variant.columns = make([]Interface, 0, totalTypes+1)
	for i := uint8(0); i < c.totalTypes; i++ {
		typeName, err := reader.Str()
		if err != nil {
			return fmt.Errorf("failed to read type name at index %d for dynamic column: %w", i, err)
		}

		c.typeNames = append(c.typeNames, typeName)
	}

	c.typeNames = append(c.typeNames, "SharedVariant")
	slices.Sort(c.typeNames)
	c.typeNamesIndex = make(map[string]int, len(c.typeNames))

	for i, typeName := range c.typeNames {
		c.typeNamesIndex[typeName] = i
		c.variant.columnTypeIndex[typeName] = uint8(i)
		if typeName == "SharedVariant" {
			typeName = "String"
		}

		col, err := Type(typeName).Column("", c.tz)
		if err != nil {
			return fmt.Errorf("failed to add dynamic column with type %s: %w", typeName, err)
		}
		c.variant.columns = append(c.variant.columns, col)
	}

	err = c.variant.decodeHeader(reader)
	if err != nil {
		return fmt.Errorf("failed to decode variant header: %w", err)
	}

	return nil
}

func (c *ColDynamic) decodeData(reader *proto.Reader, rows int) error {
	err := c.variant.decodeData(reader, rows)
	if err != nil {
		return fmt.Errorf("failed to decode variant data: %w", err)
	}

	return nil
}

func (c *ColDynamic) Decode(reader *proto.Reader, rows int) error {
	err := c.decodeHeader(reader)
	if err != nil {
		return fmt.Errorf("failed to decode dynamic header: %w", err)
	}

	err = c.decodeData(reader, rows)
	if err != nil {
		return fmt.Errorf("failed to decode dynamic data: %w", err)
	}

	return nil
}
