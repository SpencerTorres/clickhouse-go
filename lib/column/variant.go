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
	"github.com/ClickHouse/clickhouse-go/v2/lib/chcol"
	"reflect"
	"strings"
	"time"

	"github.com/ClickHouse/ch-go/proto"
)

const SupportedVariantSerializationVersion = 0
const NullVariantDiscriminator uint8 = 255

type ColVariant struct {
	chType Type
	name   string
	rows   int

	discriminators []uint8
	offsets        []int

	columns         []Interface
	columnTypeIndex map[string]uint8
}

func (c *ColVariant) parse(t Type, tz *time.Location) (_ *ColVariant, err error) {
	c.chType = t
	var (
		element       []rune
		elements      []Type
		brackets      int
		appendElement = func() {
			if len(element) != 0 {
				cType := strings.TrimSpace(string(element))
				if parts := strings.SplitN(cType, " ", 2); len(parts) == 2 {
					if !strings.Contains(parts[0], "(") {
						cType = parts[1]
					}
				}

				elements = append(elements, Type(strings.TrimSpace(cType)))
			}
		}
	)

	for _, r := range t.params() {
		switch r {
		case '(':
			brackets++
		case ')':
			brackets--
		case ',':
			if brackets == 0 {
				appendElement()
				element = element[:0]
				continue
			}
		}
		element = append(element, r)
	}

	appendElement()

	c.columnTypeIndex = make(map[string]uint8, len(elements))
	for _, columnType := range elements {
		column, err := columnType.Column("", tz)
		if err != nil {
			return nil, err
		}

		c.addColumn(column)
	}

	if len(c.columns) != 0 {
		return c, nil
	}

	return nil, &UnsupportedColumnTypeError{
		t: t,
	}
}

func (c *ColVariant) addColumn(col Interface) {
	c.columns = append(c.columns, col)
	c.columnTypeIndex[string(col.Type())] = uint8(len(c.columns) - 1)
}

func (c *ColVariant) appendDiscriminatorRow(d uint8) {
	c.discriminators = append(c.discriminators, d)
	c.rows++
}

func (c *ColVariant) appendNullRow() {
	c.appendDiscriminatorRow(NullVariantDiscriminator)
}

func (c *ColVariant) Name() string {
	return c.name
}

func (c *ColVariant) Type() Type {
	return c.chType
}

func (c *ColVariant) Rows() int {
	return c.rows
}

func (c *ColVariant) Row(i int, ptr bool) any {
	typeIndex := c.discriminators[i]
	if typeIndex == NullVariantDiscriminator {
		return nil
	}

	return c.columns[typeIndex].Row(c.offsets[i], ptr)
}

func (c *ColVariant) ScanRow(dest any, row int) error {
	typeIndex := c.discriminators[row]
	offsetIndex := c.offsets[row]
	var value any
	var chType string
	if typeIndex != NullVariantDiscriminator {
		value = c.columns[typeIndex].Row(offsetIndex, false)
		chType = string(c.columns[typeIndex].Type())
	}

	switch v := dest.(type) {
	case *chcol.Variant:
		vt := chcol.NewVariant(value)
		*v = vt
	case **chcol.Variant:
		vt := chcol.NewVariant(value)
		**v = vt
	case *chcol.VariantWithType:
		vt := chcol.NewVariantWithType(value, chType)
		*v = vt
	case **chcol.VariantWithType:
		vt := chcol.NewVariantWithType(value, chType)
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

func (c *ColVariant) Append(v any) (nulls []uint8, err error) {
	//TODO implement me
	panic("implement me")
}

func (c *ColVariant) AppendRow(v any) error {
	var requestedType string
	switch v.(type) {
	case nil:
		c.appendNullRow()
		return nil
	case chcol.VariantWithType:
		vt := v.(chcol.VariantWithType)
		requestedType = vt.Type()
		if vt.Nil() {
			c.appendNullRow()
			return nil
		}
	case *chcol.VariantWithType:
		vt := v.(*chcol.VariantWithType)
		requestedType = vt.Type()
		if vt.Nil() {
			c.appendNullRow()
			return nil
		}
	}

	if requestedType != "" {
		typeIndex, ok := c.columnTypeIndex[requestedType]
		if !ok {
			return fmt.Errorf("value %v cannot be stored in variant column %s with requested type %s: type not present in variant", v, c.chType, requestedType)
		}

		if err := c.columns[typeIndex].AppendRow(v); err != nil {
			return fmt.Errorf("failed to append row to variant column with requested type %s: %w", requestedType, err)
		}

		c.appendDiscriminatorRow(typeIndex)
		return nil
	}

	// If preferred type wasn't provided, try each column
	var err error
	for i, col := range c.columns {
		if err = col.AppendRow(v); err == nil {
			c.appendDiscriminatorRow(uint8(i))
			return nil
		}
	}

	return fmt.Errorf("value \"%v\" cannot be stored in variant column: no compatible types", v)
}

func (c *ColVariant) encodeHeader(buffer *proto.Buffer) {
	buffer.PutUInt64(SupportedVariantSerializationVersion)
}

func (c *ColVariant) encodeData(buffer *proto.Buffer) {
	buffer.PutRaw(c.discriminators)

	for _, col := range c.columns {
		col.Encode(buffer)
	}
}

func (c *ColVariant) Encode(buffer *proto.Buffer) {
	c.encodeHeader(buffer)
	c.encodeData(buffer)
}

func (c *ColVariant) ScanType() reflect.Type {
	return scanTypeVariant
}

func (c *ColVariant) Reset() {
	c.rows = 0
	c.discriminators = c.discriminators[:0]

	for _, col := range c.columns {
		col.Reset()
	}
}

func (c *ColVariant) decodeHeader(reader *proto.Reader) error {
	variantSerializationVersion, err := reader.UInt64()
	if err != nil {
		return fmt.Errorf("failed to read variant discriminator version: %w", err)
	} else if variantSerializationVersion != SupportedVariantSerializationVersion {
		return fmt.Errorf("unsupported variant discriminator version: %d", variantSerializationVersion)
	}

	return nil
}

func (c *ColVariant) decodeData(reader *proto.Reader, rows int) error {
	c.rows = rows

	c.discriminators = make([]uint8, c.rows)
	c.offsets = make([]int, c.rows)
	rowCountByType := make(map[uint8]int, len(c.columns))

	for i := 0; i < c.rows; i++ {
		disc, err := reader.ReadByte()
		if err != nil {
			return fmt.Errorf("failed to read discriminator at index %d: %w", i, err)
		}

		c.discriminators[i] = disc
		if rowCountByType[disc] == 0 {
			rowCountByType[disc] = 1
		} else {
			rowCountByType[disc]++
		}

		c.offsets[i] = rowCountByType[disc] - 1
	}

	for i, col := range c.columns {
		cRows := rowCountByType[uint8(i)]
		if err := col.Decode(reader, cRows); err != nil {
			return fmt.Errorf("failed to decode variant column with %s type: %w", col.Type(), err)
		}
	}

	return nil
}

func (c *ColVariant) Decode(reader *proto.Reader, rows int) error {
	err := c.decodeHeader(reader)
	if err != nil {
		return fmt.Errorf("failed to decode variant header: %w", err)
	}

	err = c.decodeData(reader, rows)
	if err != nil {
		return fmt.Errorf("failed to decode variant data: %w", err)
	}

	return nil
}
