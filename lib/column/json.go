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

const SupportedJSONSerializationVersion = 0
const DefaultMaxDynamicPaths = 1024

type ColJSON struct {
	chType Type
	name   string
	rows   int

	typedPaths      []string
	typedPathsIndex map[string]int
	typedColumns    []Interface

	skipPaths      []string
	skipPathsIndex map[string]int // TODO: ignore appended paths based on skip paths? does server ignore automatically?

	dynamicPaths      []string
	dynamicPathsIndex map[string]int
	dynamicColumns    []*ColDynamic

	maxDynamicPaths   int
	totalDynamicPaths int
}

func (c *ColJSON) hasTypedPath(path string) bool {
	_, ok := c.typedPathsIndex[path]
	return ok
}

func (c *ColJSON) hasDynamicPath(path string) bool {
	_, ok := c.dynamicPathsIndex[path]
	return ok
}

// pathHasNestedValues returns true if the provided path has child paths in typed or dynamic paths
// TODO: cache this information in a Set to reduce time complexity?
func (c *ColJSON) pathHasNestedValues(path string) bool {
	for _, typedPath := range c.typedPaths {
		if strings.HasPrefix(typedPath, path+".") {
			return true
		}
	}

	for _, dynamicPath := range c.dynamicPaths {
		if strings.HasPrefix(dynamicPath, path+".") {
			return true
		}
	}

	// TODO: SharedData paths

	return false
}

// valueAtPath returns the row value at the specified path, typed or dynamic
func (c *ColJSON) valueAtPath(path string, row int, ptr bool) any {
	if colIndex, ok := c.typedPathsIndex[path]; ok {
		return c.typedColumns[colIndex].Row(row, ptr)
	}

	if colIndex, ok := c.dynamicPathsIndex[path]; ok {
		return c.dynamicColumns[colIndex].Row(row, ptr)
	}

	// TODO: SharedData paths

	return nil
}

// scanTypedPathToValue scans the provided typed path into a `reflect.Value`
func (c *ColJSON) scanTypedPathToValue(path string, row int, value reflect.Value) error {
	colIndex, ok := c.typedPathsIndex[path]
	if !ok {
		return fmt.Errorf("typed path \"%s\" does not exist in JSON column", path)
	}

	col := c.typedColumns[colIndex]
	err := col.ScanRow(value.Addr().Interface(), row)
	if err != nil {
		return fmt.Errorf("failed to scan %s column into typed path \"%s\": %w", col.Type(), path, err)
	}

	return nil
}

// scanDynamicPathToValue scans the provided typed path into a `reflect.Value`
func (c *ColJSON) scanDynamicPathToValue(path string, row int, value reflect.Value) error {
	colIndex, ok := c.dynamicPathsIndex[path]
	if !ok {
		return fmt.Errorf("dynamic path \"%s\" does not exist in JSON column", path)
	}

	col := c.dynamicColumns[colIndex]
	err := col.ScanRow(value.Addr().Interface(), row)
	if err != nil {
		return fmt.Errorf("failed to scan %s column into dynamic path \"%s\": %w", col.Type(), path, err)
	}

	return nil
}

func (c *ColJSON) parse(t Type, tz *time.Location) (_ Interface, err error) {
	c.chType = t

	c.typedPathsIndex = make(map[string]int)
	c.skipPathsIndex = make(map[string]int)
	c.dynamicPathsIndex = make(map[string]int)

	c.maxDynamicPaths = DefaultMaxDynamicPaths

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
	//if len(c.typedColumns) > 0 {
	//	return c.typedColumns[0].Rows()
	//} else if len(c.dynamicColumns) > 0 {
	//	return c.dynamicColumns[0].Rows()
	//}
	//
	//return 0
	return c.rows
}

func (c *ColJSON) Row(i int, ptr bool) any {
	return nil
}

func (c *ColJSON) ScanRow(dest any, row int) error {
	switch val := reflect.ValueOf(dest); val.Kind() {
	case reflect.Ptr:
		if val.Elem().Kind() == reflect.Struct {
			return c.scanIntoStruct(dest, row)
		} else if val.Elem().Kind() == reflect.Map {
			return c.scanIntoMap(dest, row)
		}
		return fmt.Errorf("destination must be a pointer to struct or map")
	}

	// TODO: OLD STUFF

	obj := chcol.NewJSON()

	for i, path := range c.typedPaths {
		col := c.typedColumns[i]

		var value any
		err := col.ScanRow(&value, row)
		if err != nil {
			return fmt.Errorf("failed to scan json row %d for %s typed path %s", row, col.Type(), path)
		}

		obj.SetValueAtPath(path, value)
	}

	for i, path := range c.dynamicPaths {
		col := c.dynamicColumns[i]

		var value chcol.Dynamic
		err := col.ScanRow(&value, row)
		if err != nil {
			return fmt.Errorf("failed to scan json row %d for dynamic path %s", row, path)
		}

		obj.SetValueAtPath(path, value)
	}

	switch v := dest.(type) {
	case *chcol.JSON:
		*v = *obj
	case **chcol.JSON:
		**v = *obj
	default:
		return fmt.Errorf("must scan into chcol.JSON type")
	}

	return nil
}

func (c *ColJSON) Append(v any) (nulls []uint8, err error) {
	//TODO implement me
	panic("implement me")
}

func (c *ColJSON) AppendRow(v any) error {
	var obj *chcol.JSON
	var err error
	switch val := reflect.ValueOf(v); val.Kind() {
	case reflect.Pointer:
		if val.Elem().Kind() == reflect.Struct {
			obj, err = structToJSON(v)
		} else if val.Elem().Kind() == reflect.Map {
			obj, err = mapToJSON(v)
		}
	case reflect.Struct:
		obj, err = structToJSON(v)
	case reflect.Map:
		obj, err = mapToJSON(v)
	}
	if err != nil {
		return fmt.Errorf("failed to convert value to JSON: %w", err)
	}

	switch v.(type) {
	case chcol.JSON:
		vv := v.(chcol.JSON)
		obj = &vv
	case *chcol.JSON:
		obj = (v.(*chcol.JSON))
	}

	if obj == nil {
		// TODO: temp
		return fmt.Errorf("cannot append nil JSON")
	}

	// Match typed paths first
	for i, typedPath := range c.typedPaths {
		value, ok := obj.ValueAtPath(typedPath)
		if !ok {
			continue
		}

		col := c.typedColumns[i]
		err := col.AppendRow(value)
		if err != nil {
			return fmt.Errorf("failed to append type %s to json column at typed path %s: %w", col.Type(), typedPath, err)
		}
	}

	// Match or add dynamic paths
	valuesByPath := obj.ValuesByPath()
	for objPath, value := range valuesByPath {
		if c.hasTypedPath(objPath) {
			continue
		}

		if dynamicPathIndex, ok := c.dynamicPathsIndex[objPath]; ok {
			err := c.dynamicColumns[dynamicPathIndex].AppendRow(value)
			if err != nil {
				return fmt.Errorf("failed to append to json column at dynamic path \"%s\": %w", objPath, err)
			}
		} else {
			// Add new dynamic path + column
			parsedColDynamic, _ := Type("Dynamic").Column("", nil)
			colDynamic := parsedColDynamic.(*ColDynamic)

			err := colDynamic.AppendRow(value)
			if err != nil {
				return fmt.Errorf("failed to append to json column at new dynamic path \"%s\": %w", objPath, err)
			}

			c.dynamicPaths = append(c.dynamicPaths, objPath)
			c.dynamicPathsIndex[objPath] = len(c.dynamicPaths) - 1
			c.dynamicColumns = append(c.dynamicColumns, colDynamic)
			c.totalDynamicPaths++
		}
	}

	c.rows++
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

	// TODO: alphabetically sort dynamic paths for encoding!!!
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

	// TODO: shared variant goes here? per row?
	for i := 0; i < c.rows; i++ {
		buffer.PutUInt64(0)
	}
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

		dynamicPath := string(strBytes)
		c.dynamicPaths = append(c.dynamicPaths, dynamicPath)
		c.dynamicPathsIndex[dynamicPath] = len(c.dynamicPaths) - 1
	}

	for range c.typedPaths {
		// TODO: read typed path prefix (low cardinality only?)
	}

	c.dynamicColumns = make([]*ColDynamic, 0, totalDynamicPaths)
	for _, dynamicPath := range c.dynamicPaths {
		parsedColDynamic, _ := Type("Dynamic").Column("", nil)
		colDynamic := parsedColDynamic.(*ColDynamic)

		err := colDynamic.decodeHeader(reader)
		if err != nil {
			return fmt.Errorf("failed to decode dynamic header at path %s for json column: %w", dynamicPath, err)
		}

		c.dynamicColumns = append(c.dynamicColumns, colDynamic)
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
	c.rows = rows

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
