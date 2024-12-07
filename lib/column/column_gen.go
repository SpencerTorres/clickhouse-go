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

// Code generated by make codegen DO NOT EDIT.
// source: lib/column/codegen/column.tpl

package column

import (
	"database/sql"
	"database/sql/driver"
	"fmt"
	"github.com/ClickHouse/ch-go/proto"
	"github.com/google/uuid"
	"github.com/paulmach/orb"
	"github.com/shopspring/decimal"
	"math/big"
	"net"
	"reflect"
	"strings"
	"time"
)

func (t Type) Column(name string, tz *time.Location) (Interface, error) {
	switch t {
	case "Float32":
		return &Float32{name: name}, nil
	case "Float64":
		return &Float64{name: name}, nil
	case "Int8":
		return &Int8{name: name}, nil
	case "Int16":
		return &Int16{name: name}, nil
	case "Int32":
		return &Int32{name: name}, nil
	case "Int64":
		return &Int64{name: name}, nil
	case "UInt8":
		return &UInt8{name: name}, nil
	case "UInt16":
		return &UInt16{name: name}, nil
	case "UInt32":
		return &UInt32{name: name}, nil
	case "UInt64":
		return &UInt64{name: name}, nil
	case "Int128":
		return &BigInt{
			size:   16,
			chType: t,
			name:   name,
			signed: true,
			col:    &proto.ColInt128{},
		}, nil
	case "UInt128":
		return &BigInt{
			size:   16,
			chType: t,
			name:   name,
			signed: false,
			col:    &proto.ColUInt128{},
		}, nil
	case "Int256":
		return &BigInt{
			size:   32,
			chType: t,
			name:   name,
			signed: true,
			col:    &proto.ColInt256{},
		}, nil
	case "UInt256":
		return &BigInt{
			size:   32,
			chType: t,
			name:   name,
			signed: false,
			col:    &proto.ColUInt256{},
		}, nil
	case "IPv4":
		return &IPv4{name: name}, nil
	case "IPv6":
		return &IPv6{name: name}, nil
	case "Bool", "Boolean":
		return &Bool{name: name}, nil
	case "Date":
		return &Date{name: name, location: tz}, nil
	case "Date32":
		return &Date32{name: name, location: tz}, nil
	case "UUID":
		return &UUID{name: name}, nil
	case "Nothing":
		return &Nothing{name: name}, nil
	case "Ring":
		set, err := (&Array{name: name}).parse("Array(Point)", tz)
		if err != nil {
			return nil, err
		}
		set.chType = "Ring"
		return &Ring{
			set:  set,
			name: name,
		}, nil
	case "Polygon":
		set, err := (&Array{name: name}).parse("Array(Ring)", tz)
		if err != nil {
			return nil, err
		}
		set.chType = "Polygon"
		return &Polygon{
			set:  set,
			name: name,
		}, nil
	case "MultiPolygon":
		set, err := (&Array{name: name}).parse("Array(Polygon)", tz)
		if err != nil {
			return nil, err
		}
		set.chType = "MultiPolygon"
		return &MultiPolygon{
			set:  set,
			name: name,
		}, nil
	case "Point":
		return &Point{name: name}, nil
	case "String":
		return &String{name: name}, nil
	case "Object('json')":
		return &JSONObject{name: name, root: true, tz: tz}, nil
	}

	switch strType := string(t); {
	case strings.HasPrefix(string(t), "Map("):
		return (&Map{name: name}).parse(t, tz)
	case strings.HasPrefix(string(t), "Tuple("):
		return (&Tuple{name: name}).parse(t, tz)
	case strings.HasPrefix(string(t), "Variant("):
		return (&ColVariant{name: name}).parse(t, tz)
	case strings.HasPrefix(string(t), "Dynamic"):
		return (&ColDynamic{name: name}).parse(t, tz)
	case strings.HasPrefix(string(t), "JSON"):
		return (&ColJSON{name: name}).parse(t, tz)
	case strings.HasPrefix(string(t), "Decimal("):
		return (&Decimal{name: name}).parse(t)
	case strings.HasPrefix(strType, "Nested("):
		return (&Nested{name: name}).parse(t, tz)
	case strings.HasPrefix(string(t), "Array("):
		return (&Array{name: name}).parse(t, tz)
	case strings.HasPrefix(string(t), "Interval"):
		return (&Interval{name: name}).parse(t)
	case strings.HasPrefix(string(t), "Nullable"):
		return (&Nullable{name: name}).parse(t, tz)
	case strings.HasPrefix(string(t), "FixedString"):
		return (&FixedString{name: name}).parse(t)
	case strings.HasPrefix(string(t), "LowCardinality"):
		return (&LowCardinality{name: name}).parse(t, tz)
	case strings.HasPrefix(string(t), "SimpleAggregateFunction"):
		return (&SimpleAggregateFunction{name: name}).parse(t, tz)
	case strings.HasPrefix(string(t), "Enum8") || strings.HasPrefix(string(t), "Enum16"):
		return Enum(t, name)
	case strings.HasPrefix(string(t), "DateTime64"):
		return (&DateTime64{name: name}).parse(t, tz)
	case strings.HasPrefix(strType, "DateTime") && !strings.HasPrefix(strType, "DateTime64"):
		return (&DateTime{name: name}).parse(t, tz)
	}
	return nil, &UnsupportedColumnTypeError{
		t: t,
	}
}

type (
	Float32 struct {
		name string
		col  proto.ColFloat32
	}
	Float64 struct {
		name string
		col  proto.ColFloat64
	}
	Int8 struct {
		name string
		col  proto.ColInt8
	}
	Int16 struct {
		name string
		col  proto.ColInt16
	}
	Int32 struct {
		name string
		col  proto.ColInt32
	}
	Int64 struct {
		name string
		col  proto.ColInt64
	}
	UInt8 struct {
		name string
		col  proto.ColUInt8
	}
	UInt16 struct {
		name string
		col  proto.ColUInt16
	}
	UInt32 struct {
		name string
		col  proto.ColUInt32
	}
	UInt64 struct {
		name string
		col  proto.ColUInt64
	}
)

var (
	_ Interface = (*Float32)(nil)
	_ Interface = (*Float64)(nil)
	_ Interface = (*Int8)(nil)
	_ Interface = (*Int16)(nil)
	_ Interface = (*Int32)(nil)
	_ Interface = (*Int64)(nil)
	_ Interface = (*UInt8)(nil)
	_ Interface = (*UInt16)(nil)
	_ Interface = (*UInt32)(nil)
	_ Interface = (*UInt64)(nil)
)

var (
	scanTypeFloat32      = reflect.TypeOf(float32(0))
	scanTypeFloat64      = reflect.TypeOf(float64(0))
	scanTypeInt8         = reflect.TypeOf(int8(0))
	scanTypeInt16        = reflect.TypeOf(int16(0))
	scanTypeInt32        = reflect.TypeOf(int32(0))
	scanTypeInt64        = reflect.TypeOf(int64(0))
	scanTypeUInt8        = reflect.TypeOf(uint8(0))
	scanTypeUInt16       = reflect.TypeOf(uint16(0))
	scanTypeUInt32       = reflect.TypeOf(uint32(0))
	scanTypeUInt64       = reflect.TypeOf(uint64(0))
	scanTypeIP           = reflect.TypeOf(net.IP{})
	scanTypeBool         = reflect.TypeOf(true)
	scanTypeByte         = reflect.TypeOf([]byte{})
	scanTypeUUID         = reflect.TypeOf(uuid.UUID{})
	scanTypeTime         = reflect.TypeOf(time.Time{})
	scanTypeRing         = reflect.TypeOf(orb.Ring{})
	scanTypePoint        = reflect.TypeOf(orb.Point{})
	scanTypeSlice        = reflect.TypeOf([]any{})
	scanTypeMap          = reflect.TypeOf(map[string]any{})
	scanTypeBigInt       = reflect.TypeOf(&big.Int{})
	scanTypeString       = reflect.TypeOf("")
	scanTypePolygon      = reflect.TypeOf(orb.Polygon{})
	scanTypeDecimal      = reflect.TypeOf(decimal.Decimal{})
	scanTypeMultiPolygon = reflect.TypeOf(orb.MultiPolygon{})
)

func (col *Float32) Name() string {
	return col.name
}

func (col *Float32) Type() Type {
	return "Float32"
}

func (col *Float32) ScanType() reflect.Type {
	return scanTypeFloat32
}

func (col *Float32) Rows() int {
	return col.col.Rows()
}

func (col *Float32) Reset() {
	col.col.Reset()
}

func (col *Float32) ScanRow(dest any, row int) error {
	value := col.col.Row(row)
	switch d := dest.(type) {
	case *float32:
		*d = value
	case **float32:
		*d = new(float32)
		**d = value
	default:
		if scan, ok := dest.(sql.Scanner); ok {
			return scan.Scan(value)
		}
		return &ColumnConverterError{
			Op:   "ScanRow",
			To:   fmt.Sprintf("%T", dest),
			From: "Float32",
			Hint: fmt.Sprintf("try using *%s", scanTypeFloat32),
		}
	}
	return nil
}

func (col *Float32) Row(i int, ptr bool) any {
	value := col.col.Row(i)
	if ptr {
		return &value
	}
	return value
}

func (col *Float32) Append(v any) (nulls []uint8, err error) {
	switch v := v.(type) {
	case []float32:
		nulls = make([]uint8, len(v))
		for i := range v {
			col.col.Append(v[i])
		}
	case []*float32:
		nulls = make([]uint8, len(v))
		for i := range v {
			switch {
			case v[i] != nil:
				col.col.Append(*v[i])
			default:
				col.col.Append(0)
				nulls[i] = 1
			}
		}
	default:

		if valuer, ok := v.(driver.Valuer); ok {
			val, err := valuer.Value()
			if err != nil {
				return nil, &ColumnConverterError{
					Op:   "Append",
					To:   "Float32",
					From: fmt.Sprintf("%T", v),
					Hint: "could not get driver.Valuer value",
				}
			}
			return col.Append(val)
		}

		return nil, &ColumnConverterError{
			Op:   "Append",
			To:   "Float32",
			From: fmt.Sprintf("%T", v),
		}
	}
	return
}

func (col *Float32) AppendRow(v any) error {
	switch v := v.(type) {
	case float32:
		col.col.Append(v)
	case *float32:
		switch {
		case v != nil:
			col.col.Append(*v)
		default:
			col.col.Append(0)
		}
	case nil:
		col.col.Append(0)
	default:

		if valuer, ok := v.(driver.Valuer); ok {
			val, err := valuer.Value()
			if err != nil {
				return &ColumnConverterError{
					Op:   "AppendRow",
					To:   "Float32",
					From: fmt.Sprintf("%T", v),
					Hint: "could not get driver.Valuer value",
				}
			}
			return col.AppendRow(val)
		}

		if rv := reflect.ValueOf(v); rv.Kind() == col.ScanType().Kind() || rv.CanConvert(col.ScanType()) {
			col.col.Append(rv.Convert(col.ScanType()).Interface().(float32))
		} else {
			return &ColumnConverterError{
				Op:   "AppendRow",
				To:   "Float32",
				From: fmt.Sprintf("%T", v),
			}
		}
	}
	return nil
}

func (col *Float32) Decode(reader *proto.Reader, rows int) error {
	return col.col.DecodeColumn(reader, rows)
}

func (col *Float32) Encode(buffer *proto.Buffer) {
	col.col.EncodeColumn(buffer)
}

func (col *Float64) Name() string {
	return col.name
}

func (col *Float64) Type() Type {
	return "Float64"
}

func (col *Float64) ScanType() reflect.Type {
	return scanTypeFloat64
}

func (col *Float64) Rows() int {
	return col.col.Rows()
}

func (col *Float64) Reset() {
	col.col.Reset()
}

func (col *Float64) ScanRow(dest any, row int) error {
	value := col.col.Row(row)
	switch d := dest.(type) {
	case *float64:
		*d = value
	case **float64:
		*d = new(float64)
		**d = value
	case *sql.NullFloat64:
		return d.Scan(value)
	default:
		if scan, ok := dest.(sql.Scanner); ok {
			return scan.Scan(value)
		}
		return &ColumnConverterError{
			Op:   "ScanRow",
			To:   fmt.Sprintf("%T", dest),
			From: "Float64",
			Hint: fmt.Sprintf("try using *%s", scanTypeFloat64),
		}
	}
	return nil
}

func (col *Float64) Row(i int, ptr bool) any {
	value := col.col.Row(i)
	if ptr {
		return &value
	}
	return value
}

func (col *Float64) Append(v any) (nulls []uint8, err error) {
	switch v := v.(type) {
	case []float64:
		nulls = make([]uint8, len(v))
		for i := range v {
			col.col.Append(v[i])
		}
	case []*float64:
		nulls = make([]uint8, len(v))
		for i := range v {
			switch {
			case v[i] != nil:
				col.col.Append(*v[i])
			default:
				col.col.Append(0)
				nulls[i] = 1
			}
		}
	case []sql.NullFloat64:
		nulls = make([]uint8, len(v))
		for i := range v {
			col.AppendRow(v[i])
		}
	case []*sql.NullFloat64:
		nulls = make([]uint8, len(v))
		for i := range v {
			if v[i] == nil {
				nulls[i] = 1
			}
			col.AppendRow(v[i])
		}
	default:

		if valuer, ok := v.(driver.Valuer); ok {
			val, err := valuer.Value()
			if err != nil {
				return nil, &ColumnConverterError{
					Op:   "Append",
					To:   "Float64",
					From: fmt.Sprintf("%T", v),
					Hint: "could not get driver.Valuer value",
				}
			}
			return col.Append(val)
		}

		return nil, &ColumnConverterError{
			Op:   "Append",
			To:   "Float64",
			From: fmt.Sprintf("%T", v),
		}
	}
	return
}

func (col *Float64) AppendRow(v any) error {
	switch v := v.(type) {
	case float64:
		col.col.Append(v)
	case *float64:
		switch {
		case v != nil:
			col.col.Append(*v)
		default:
			col.col.Append(0)
		}
	case nil:
		col.col.Append(0)
	case sql.NullFloat64:
		switch v.Valid {
		case true:
			col.col.Append(v.Float64)
		default:
			col.col.Append(0)
		}
	case *sql.NullFloat64:
		switch v.Valid {
		case true:
			col.col.Append(v.Float64)
		default:
			col.col.Append(0)
		}
	default:

		if valuer, ok := v.(driver.Valuer); ok {
			val, err := valuer.Value()
			if err != nil {
				return &ColumnConverterError{
					Op:   "AppendRow",
					To:   "Float64",
					From: fmt.Sprintf("%T", v),
					Hint: "could not get driver.Valuer value",
				}
			}
			return col.AppendRow(val)
		}

		if rv := reflect.ValueOf(v); rv.Kind() == col.ScanType().Kind() || rv.CanConvert(col.ScanType()) {
			col.col.Append(rv.Convert(col.ScanType()).Interface().(float64))
		} else {
			return &ColumnConverterError{
				Op:   "AppendRow",
				To:   "Float64",
				From: fmt.Sprintf("%T", v),
			}
		}
	}
	return nil
}

func (col *Float64) Decode(reader *proto.Reader, rows int) error {
	return col.col.DecodeColumn(reader, rows)
}

func (col *Float64) Encode(buffer *proto.Buffer) {
	col.col.EncodeColumn(buffer)
}

func (col *Int8) Name() string {
	return col.name
}

func (col *Int8) Type() Type {
	return "Int8"
}

func (col *Int8) ScanType() reflect.Type {
	return scanTypeInt8
}

func (col *Int8) Rows() int {
	return col.col.Rows()
}

func (col *Int8) Reset() {
	col.col.Reset()
}

func (col *Int8) ScanRow(dest any, row int) error {
	value := col.col.Row(row)
	switch d := dest.(type) {
	case *int8:
		*d = value
	case **int8:
		*d = new(int8)
		**d = value
	case *bool:
		switch value {
		case 0:
			*d = false
		default:
			*d = true
		}
	default:
		if scan, ok := dest.(sql.Scanner); ok {
			return scan.Scan(value)
		}
		return &ColumnConverterError{
			Op:   "ScanRow",
			To:   fmt.Sprintf("%T", dest),
			From: "Int8",
			Hint: fmt.Sprintf("try using *%s", scanTypeInt8),
		}
	}
	return nil
}

func (col *Int8) Row(i int, ptr bool) any {
	value := col.col.Row(i)
	if ptr {
		return &value
	}
	return value
}

func (col *Int8) Append(v any) (nulls []uint8, err error) {
	switch v := v.(type) {
	case []int8:
		nulls = make([]uint8, len(v))
		for i := range v {
			col.col.Append(v[i])
		}
	case []*int8:
		nulls = make([]uint8, len(v))
		for i := range v {
			switch {
			case v[i] != nil:
				col.col.Append(*v[i])
			default:
				col.col.Append(0)
				nulls[i] = 1
			}
		}
	case []bool:
		nulls = make([]uint8, len(v))
		for i := range v {
			val := int8(0)
			if v[i] {
				val = 1
			}
			col.col.Append(val)
		}
	case []*bool:
		nulls = make([]uint8, len(v))
		for i := range v {
			val := int8(0)
			if v[i] == nil {
				nulls[i] = 1
			} else if *v[i] {
				val = 1
			}
			col.col.Append(val)
		}
	default:

		if valuer, ok := v.(driver.Valuer); ok {
			val, err := valuer.Value()
			if err != nil {
				return nil, &ColumnConverterError{
					Op:   "Append",
					To:   "Int8",
					From: fmt.Sprintf("%T", v),
					Hint: "could not get driver.Valuer value",
				}
			}
			return col.Append(val)
		}

		return nil, &ColumnConverterError{
			Op:   "Append",
			To:   "Int8",
			From: fmt.Sprintf("%T", v),
		}
	}
	return
}

func (col *Int8) AppendRow(v any) error {
	switch v := v.(type) {
	case int8:
		col.col.Append(v)
	case *int8:
		switch {
		case v != nil:
			col.col.Append(*v)
		default:
			col.col.Append(0)
		}
	case nil:
		col.col.Append(0)
	case bool:
		val := int8(0)
		if v {
			val = 1
		}
		col.col.Append(val)
	case *bool:
		val := int8(0)
		if *v {
			val = 1
		}
		col.col.Append(val)
	default:

		if valuer, ok := v.(driver.Valuer); ok {
			val, err := valuer.Value()
			if err != nil {
				return &ColumnConverterError{
					Op:   "AppendRow",
					To:   "Int8",
					From: fmt.Sprintf("%T", v),
					Hint: "could not get driver.Valuer value",
				}
			}
			return col.AppendRow(val)
		}

		if rv := reflect.ValueOf(v); rv.Kind() == col.ScanType().Kind() || rv.CanConvert(col.ScanType()) {
			col.col.Append(rv.Convert(col.ScanType()).Interface().(int8))
		} else {
			return &ColumnConverterError{
				Op:   "AppendRow",
				To:   "Int8",
				From: fmt.Sprintf("%T", v),
			}
		}
	}
	return nil
}

func (col *Int8) Decode(reader *proto.Reader, rows int) error {
	return col.col.DecodeColumn(reader, rows)
}

func (col *Int8) Encode(buffer *proto.Buffer) {
	col.col.EncodeColumn(buffer)
}

func (col *Int16) Name() string {
	return col.name
}

func (col *Int16) Type() Type {
	return "Int16"
}

func (col *Int16) ScanType() reflect.Type {
	return scanTypeInt16
}

func (col *Int16) Rows() int {
	return col.col.Rows()
}

func (col *Int16) Reset() {
	col.col.Reset()
}

func (col *Int16) ScanRow(dest any, row int) error {
	value := col.col.Row(row)
	switch d := dest.(type) {
	case *int16:
		*d = value
	case **int16:
		*d = new(int16)
		**d = value
	case *sql.NullInt16:
		return d.Scan(value)
	default:
		if scan, ok := dest.(sql.Scanner); ok {
			return scan.Scan(value)
		}
		return &ColumnConverterError{
			Op:   "ScanRow",
			To:   fmt.Sprintf("%T", dest),
			From: "Int16",
			Hint: fmt.Sprintf("try using *%s", scanTypeInt16),
		}
	}
	return nil
}

func (col *Int16) Row(i int, ptr bool) any {
	value := col.col.Row(i)
	if ptr {
		return &value
	}
	return value
}

func (col *Int16) Append(v any) (nulls []uint8, err error) {
	switch v := v.(type) {
	case []int16:
		nulls = make([]uint8, len(v))
		for i := range v {
			col.col.Append(v[i])
		}
	case []*int16:
		nulls = make([]uint8, len(v))
		for i := range v {
			switch {
			case v[i] != nil:
				col.col.Append(*v[i])
			default:
				col.col.Append(0)
				nulls[i] = 1
			}
		}
	case []sql.NullInt16:
		nulls = make([]uint8, len(v))
		for i := range v {
			col.AppendRow(v[i])
		}
	case []*sql.NullInt16:
		nulls = make([]uint8, len(v))
		for i := range v {
			if v[i] == nil {
				nulls[i] = 1
			}
			col.AppendRow(v[i])
		}
	default:

		if valuer, ok := v.(driver.Valuer); ok {
			val, err := valuer.Value()
			if err != nil {
				return nil, &ColumnConverterError{
					Op:   "Append",
					To:   "Int16",
					From: fmt.Sprintf("%T", v),
					Hint: "could not get driver.Valuer value",
				}
			}
			return col.Append(val)
		}

		return nil, &ColumnConverterError{
			Op:   "Append",
			To:   "Int16",
			From: fmt.Sprintf("%T", v),
		}
	}
	return
}

func (col *Int16) AppendRow(v any) error {
	switch v := v.(type) {
	case int16:
		col.col.Append(v)
	case *int16:
		switch {
		case v != nil:
			col.col.Append(*v)
		default:
			col.col.Append(0)
		}
	case nil:
		col.col.Append(0)
	case sql.NullInt16:
		switch v.Valid {
		case true:
			col.col.Append(v.Int16)
		default:
			col.col.Append(0)
		}
	case *sql.NullInt16:
		switch v.Valid {
		case true:
			col.col.Append(v.Int16)
		default:
			col.col.Append(0)
		}
	default:

		if valuer, ok := v.(driver.Valuer); ok {
			val, err := valuer.Value()
			if err != nil {
				return &ColumnConverterError{
					Op:   "AppendRow",
					To:   "Int16",
					From: fmt.Sprintf("%T", v),
					Hint: "could not get driver.Valuer value",
				}
			}
			return col.AppendRow(val)
		}

		if rv := reflect.ValueOf(v); rv.Kind() == col.ScanType().Kind() || rv.CanConvert(col.ScanType()) {
			col.col.Append(rv.Convert(col.ScanType()).Interface().(int16))
		} else {
			return &ColumnConverterError{
				Op:   "AppendRow",
				To:   "Int16",
				From: fmt.Sprintf("%T", v),
			}
		}
	}
	return nil
}

func (col *Int16) Decode(reader *proto.Reader, rows int) error {
	return col.col.DecodeColumn(reader, rows)
}

func (col *Int16) Encode(buffer *proto.Buffer) {
	col.col.EncodeColumn(buffer)
}

func (col *Int32) Name() string {
	return col.name
}

func (col *Int32) Type() Type {
	return "Int32"
}

func (col *Int32) ScanType() reflect.Type {
	return scanTypeInt32
}

func (col *Int32) Rows() int {
	return col.col.Rows()
}

func (col *Int32) Reset() {
	col.col.Reset()
}

func (col *Int32) ScanRow(dest any, row int) error {
	value := col.col.Row(row)
	switch d := dest.(type) {
	case *int32:
		*d = value
	case **int32:
		*d = new(int32)
		**d = value
	case *sql.NullInt32:
		return d.Scan(value)
	default:
		if scan, ok := dest.(sql.Scanner); ok {
			return scan.Scan(value)
		}
		return &ColumnConverterError{
			Op:   "ScanRow",
			To:   fmt.Sprintf("%T", dest),
			From: "Int32",
			Hint: fmt.Sprintf("try using *%s", scanTypeInt32),
		}
	}
	return nil
}

func (col *Int32) Row(i int, ptr bool) any {
	value := col.col.Row(i)
	if ptr {
		return &value
	}
	return value
}

func (col *Int32) Append(v any) (nulls []uint8, err error) {
	switch v := v.(type) {
	case []int32:
		nulls = make([]uint8, len(v))
		for i := range v {
			col.col.Append(v[i])
		}
	case []*int32:
		nulls = make([]uint8, len(v))
		for i := range v {
			switch {
			case v[i] != nil:
				col.col.Append(*v[i])
			default:
				col.col.Append(0)
				nulls[i] = 1
			}
		}
	case []sql.NullInt32:
		nulls = make([]uint8, len(v))
		for i := range v {
			col.AppendRow(v[i])
		}
	case []*sql.NullInt32:
		nulls = make([]uint8, len(v))
		for i := range v {
			if v[i] == nil {
				nulls[i] = 1
			}
			col.AppendRow(v[i])
		}
	default:

		if valuer, ok := v.(driver.Valuer); ok {
			val, err := valuer.Value()
			if err != nil {
				return nil, &ColumnConverterError{
					Op:   "Append",
					To:   "Int32",
					From: fmt.Sprintf("%T", v),
					Hint: "could not get driver.Valuer value",
				}
			}
			return col.Append(val)
		}

		return nil, &ColumnConverterError{
			Op:   "Append",
			To:   "Int32",
			From: fmt.Sprintf("%T", v),
		}
	}
	return
}

func (col *Int32) AppendRow(v any) error {
	switch v := v.(type) {
	case int32:
		col.col.Append(v)
	case *int32:
		switch {
		case v != nil:
			col.col.Append(*v)
		default:
			col.col.Append(0)
		}
	case nil:
		col.col.Append(0)
	case sql.NullInt32:
		switch v.Valid {
		case true:
			col.col.Append(v.Int32)
		default:
			col.col.Append(0)
		}
	case *sql.NullInt32:
		switch v.Valid {
		case true:
			col.col.Append(v.Int32)
		default:
			col.col.Append(0)
		}
	default:

		if valuer, ok := v.(driver.Valuer); ok {
			val, err := valuer.Value()
			if err != nil {
				return &ColumnConverterError{
					Op:   "AppendRow",
					To:   "Int32",
					From: fmt.Sprintf("%T", v),
					Hint: "could not get driver.Valuer value",
				}
			}
			return col.AppendRow(val)
		}

		if rv := reflect.ValueOf(v); rv.Kind() == col.ScanType().Kind() || rv.CanConvert(col.ScanType()) {
			col.col.Append(rv.Convert(col.ScanType()).Interface().(int32))
		} else {
			return &ColumnConverterError{
				Op:   "AppendRow",
				To:   "Int32",
				From: fmt.Sprintf("%T", v),
			}
		}
	}
	return nil
}

func (col *Int32) Decode(reader *proto.Reader, rows int) error {
	return col.col.DecodeColumn(reader, rows)
}

func (col *Int32) Encode(buffer *proto.Buffer) {
	col.col.EncodeColumn(buffer)
}

func (col *Int64) Name() string {
	return col.name
}

func (col *Int64) Type() Type {
	return "Int64"
}

func (col *Int64) ScanType() reflect.Type {
	return scanTypeInt64
}

func (col *Int64) Rows() int {
	return col.col.Rows()
}

func (col *Int64) Reset() {
	col.col.Reset()
}

func (col *Int64) ScanRow(dest any, row int) error {
	value := col.col.Row(row)
	switch d := dest.(type) {
	case *int64:
		*d = value
	case **int64:
		*d = new(int64)
		**d = value
	case *time.Duration:
		*d = time.Duration(value)
	case *sql.NullInt64:
		return d.Scan(value)
	default:
		if scan, ok := dest.(sql.Scanner); ok {
			return scan.Scan(value)
		}
		return &ColumnConverterError{
			Op:   "ScanRow",
			To:   fmt.Sprintf("%T", dest),
			From: "Int64",
			Hint: fmt.Sprintf("try using *%s", scanTypeInt64),
		}
	}
	return nil
}

func (col *Int64) Row(i int, ptr bool) any {
	value := col.col.Row(i)
	if ptr {
		return &value
	}
	return value
}

func (col *Int64) Append(v any) (nulls []uint8, err error) {
	switch v := v.(type) {
	case []int64:
		nulls = make([]uint8, len(v))
		for i := range v {
			col.col.Append(v[i])
		}
	case []*int64:
		nulls = make([]uint8, len(v))
		for i := range v {
			switch {
			case v[i] != nil:
				col.col.Append(*v[i])
			default:
				col.col.Append(0)
				nulls[i] = 1
			}
		}
	case []sql.NullInt64:
		nulls = make([]uint8, len(v))
		for i := range v {
			col.AppendRow(v[i])
		}
	case []*sql.NullInt64:
		nulls = make([]uint8, len(v))
		for i := range v {
			if v[i] == nil {
				nulls[i] = 1
			}
			col.AppendRow(v[i])
		}
	default:

		if valuer, ok := v.(driver.Valuer); ok {
			val, err := valuer.Value()
			if err != nil {
				return nil, &ColumnConverterError{
					Op:   "Append",
					To:   "Int64",
					From: fmt.Sprintf("%T", v),
					Hint: "could not get driver.Valuer value",
				}
			}
			return col.Append(val)
		}

		return nil, &ColumnConverterError{
			Op:   "Append",
			To:   "Int64",
			From: fmt.Sprintf("%T", v),
		}
	}
	return
}

func (col *Int64) AppendRow(v any) error {
	switch v := v.(type) {
	case int64:
		col.col.Append(v)
	case *int64:
		switch {
		case v != nil:
			col.col.Append(*v)
		default:
			col.col.Append(0)
		}
	case nil:
		col.col.Append(0)
	case sql.NullInt64:
		switch v.Valid {
		case true:
			col.col.Append(v.Int64)
		default:
			col.col.Append(0)
		}
	case *sql.NullInt64:
		switch v.Valid {
		case true:
			col.col.Append(v.Int64)
		default:
			col.col.Append(0)
		}
	case time.Duration:
		col.col.Append(int64(v))
	case *time.Duration:
		col.col.Append(int64(*v))
	default:

		if valuer, ok := v.(driver.Valuer); ok {
			val, err := valuer.Value()
			if err != nil {
				return &ColumnConverterError{
					Op:   "AppendRow",
					To:   "Int64",
					From: fmt.Sprintf("%T", v),
					Hint: "could not get driver.Valuer value",
				}
			}
			return col.AppendRow(val)
		}

		if rv := reflect.ValueOf(v); rv.Kind() == col.ScanType().Kind() || rv.CanConvert(col.ScanType()) {
			col.col.Append(rv.Convert(col.ScanType()).Interface().(int64))
		} else {
			return &ColumnConverterError{
				Op:   "AppendRow",
				To:   "Int64",
				From: fmt.Sprintf("%T", v),
			}
		}
	}
	return nil
}

func (col *Int64) Decode(reader *proto.Reader, rows int) error {
	return col.col.DecodeColumn(reader, rows)
}

func (col *Int64) Encode(buffer *proto.Buffer) {
	col.col.EncodeColumn(buffer)
}

func (col *UInt8) Name() string {
	return col.name
}

func (col *UInt8) Type() Type {
	return "UInt8"
}

func (col *UInt8) ScanType() reflect.Type {
	return scanTypeUInt8
}

func (col *UInt8) Rows() int {
	return col.col.Rows()
}

func (col *UInt8) Reset() {
	col.col.Reset()
}

func (col *UInt8) ScanRow(dest any, row int) error {
	value := col.col.Row(row)
	switch d := dest.(type) {
	case *uint8:
		*d = value
	case **uint8:
		*d = new(uint8)
		**d = value
	case *bool:
		switch value {
		case 0:
			*d = false
		default:
			*d = true
		}
	default:
		if scan, ok := dest.(sql.Scanner); ok {
			return scan.Scan(value)
		}
		return &ColumnConverterError{
			Op:   "ScanRow",
			To:   fmt.Sprintf("%T", dest),
			From: "UInt8",
			Hint: fmt.Sprintf("try using *%s", scanTypeUInt8),
		}
	}
	return nil
}

func (col *UInt8) Row(i int, ptr bool) any {
	value := col.col.Row(i)
	if ptr {
		return &value
	}
	return value
}

func (col *UInt8) Append(v any) (nulls []uint8, err error) {
	switch v := v.(type) {
	case []uint8:
		nulls = make([]uint8, len(v))
		for i := range v {
			col.col.Append(v[i])
		}
	case []*uint8:
		nulls = make([]uint8, len(v))
		for i := range v {
			switch {
			case v[i] != nil:
				col.col.Append(*v[i])
			default:
				col.col.Append(0)
				nulls[i] = 1
			}
		}
	default:

		if valuer, ok := v.(driver.Valuer); ok {
			val, err := valuer.Value()
			if err != nil {
				return nil, &ColumnConverterError{
					Op:   "Append",
					To:   "UInt8",
					From: fmt.Sprintf("%T", v),
					Hint: "could not get driver.Valuer value",
				}
			}
			return col.Append(val)
		}

		return nil, &ColumnConverterError{
			Op:   "Append",
			To:   "UInt8",
			From: fmt.Sprintf("%T", v),
		}
	}
	return
}

func (col *UInt8) AppendRow(v any) error {
	switch v := v.(type) {
	case uint8:
		col.col.Append(v)
	case *uint8:
		switch {
		case v != nil:
			col.col.Append(*v)
		default:
			col.col.Append(0)
		}
	case nil:
		col.col.Append(0)
	case bool:
		var t uint8
		if v {
			t = 1
		}
		col.col.Append(t)
	default:

		if valuer, ok := v.(driver.Valuer); ok {
			val, err := valuer.Value()
			if err != nil {
				return &ColumnConverterError{
					Op:   "AppendRow",
					To:   "UInt8",
					From: fmt.Sprintf("%T", v),
					Hint: "could not get driver.Valuer value",
				}
			}
			return col.AppendRow(val)
		}

		if rv := reflect.ValueOf(v); rv.Kind() == col.ScanType().Kind() || rv.CanConvert(col.ScanType()) {
			col.col.Append(rv.Convert(col.ScanType()).Interface().(uint8))
		} else {
			return &ColumnConverterError{
				Op:   "AppendRow",
				To:   "UInt8",
				From: fmt.Sprintf("%T", v),
			}
		}
	}
	return nil
}

func (col *UInt8) Decode(reader *proto.Reader, rows int) error {
	return col.col.DecodeColumn(reader, rows)
}

func (col *UInt8) Encode(buffer *proto.Buffer) {
	col.col.EncodeColumn(buffer)
}

func (col *UInt16) Name() string {
	return col.name
}

func (col *UInt16) Type() Type {
	return "UInt16"
}

func (col *UInt16) ScanType() reflect.Type {
	return scanTypeUInt16
}

func (col *UInt16) Rows() int {
	return col.col.Rows()
}

func (col *UInt16) Reset() {
	col.col.Reset()
}

func (col *UInt16) ScanRow(dest any, row int) error {
	value := col.col.Row(row)
	switch d := dest.(type) {
	case *uint16:
		*d = value
	case **uint16:
		*d = new(uint16)
		**d = value
	default:
		if scan, ok := dest.(sql.Scanner); ok {
			return scan.Scan(value)
		}
		return &ColumnConverterError{
			Op:   "ScanRow",
			To:   fmt.Sprintf("%T", dest),
			From: "UInt16",
			Hint: fmt.Sprintf("try using *%s", scanTypeUInt16),
		}
	}
	return nil
}

func (col *UInt16) Row(i int, ptr bool) any {
	value := col.col.Row(i)
	if ptr {
		return &value
	}
	return value
}

func (col *UInt16) Append(v any) (nulls []uint8, err error) {
	switch v := v.(type) {
	case []uint16:
		nulls = make([]uint8, len(v))
		for i := range v {
			col.col.Append(v[i])
		}
	case []*uint16:
		nulls = make([]uint8, len(v))
		for i := range v {
			switch {
			case v[i] != nil:
				col.col.Append(*v[i])
			default:
				col.col.Append(0)
				nulls[i] = 1
			}
		}
	default:

		if valuer, ok := v.(driver.Valuer); ok {
			val, err := valuer.Value()
			if err != nil {
				return nil, &ColumnConverterError{
					Op:   "Append",
					To:   "UInt16",
					From: fmt.Sprintf("%T", v),
					Hint: "could not get driver.Valuer value",
				}
			}
			return col.Append(val)
		}

		return nil, &ColumnConverterError{
			Op:   "Append",
			To:   "UInt16",
			From: fmt.Sprintf("%T", v),
		}
	}
	return
}

func (col *UInt16) AppendRow(v any) error {
	switch v := v.(type) {
	case uint16:
		col.col.Append(v)
	case *uint16:
		switch {
		case v != nil:
			col.col.Append(*v)
		default:
			col.col.Append(0)
		}
	case nil:
		col.col.Append(0)
	default:

		if valuer, ok := v.(driver.Valuer); ok {
			val, err := valuer.Value()
			if err != nil {
				return &ColumnConverterError{
					Op:   "AppendRow",
					To:   "UInt16",
					From: fmt.Sprintf("%T", v),
					Hint: "could not get driver.Valuer value",
				}
			}
			return col.AppendRow(val)
		}

		if rv := reflect.ValueOf(v); rv.Kind() == col.ScanType().Kind() || rv.CanConvert(col.ScanType()) {
			col.col.Append(rv.Convert(col.ScanType()).Interface().(uint16))
		} else {
			return &ColumnConverterError{
				Op:   "AppendRow",
				To:   "UInt16",
				From: fmt.Sprintf("%T", v),
			}
		}
	}
	return nil
}

func (col *UInt16) Decode(reader *proto.Reader, rows int) error {
	return col.col.DecodeColumn(reader, rows)
}

func (col *UInt16) Encode(buffer *proto.Buffer) {
	col.col.EncodeColumn(buffer)
}

func (col *UInt32) Name() string {
	return col.name
}

func (col *UInt32) Type() Type {
	return "UInt32"
}

func (col *UInt32) ScanType() reflect.Type {
	return scanTypeUInt32
}

func (col *UInt32) Rows() int {
	return col.col.Rows()
}

func (col *UInt32) Reset() {
	col.col.Reset()
}

func (col *UInt32) ScanRow(dest any, row int) error {
	value := col.col.Row(row)
	switch d := dest.(type) {
	case *uint32:
		*d = value
	case **uint32:
		*d = new(uint32)
		**d = value
	default:
		if scan, ok := dest.(sql.Scanner); ok {
			return scan.Scan(value)
		}
		return &ColumnConverterError{
			Op:   "ScanRow",
			To:   fmt.Sprintf("%T", dest),
			From: "UInt32",
			Hint: fmt.Sprintf("try using *%s", scanTypeUInt32),
		}
	}
	return nil
}

func (col *UInt32) Row(i int, ptr bool) any {
	value := col.col.Row(i)
	if ptr {
		return &value
	}
	return value
}

func (col *UInt32) Append(v any) (nulls []uint8, err error) {
	switch v := v.(type) {
	case []uint32:
		nulls = make([]uint8, len(v))
		for i := range v {
			col.col.Append(v[i])
		}
	case []*uint32:
		nulls = make([]uint8, len(v))
		for i := range v {
			switch {
			case v[i] != nil:
				col.col.Append(*v[i])
			default:
				col.col.Append(0)
				nulls[i] = 1
			}
		}
	default:

		if valuer, ok := v.(driver.Valuer); ok {
			val, err := valuer.Value()
			if err != nil {
				return nil, &ColumnConverterError{
					Op:   "Append",
					To:   "UInt32",
					From: fmt.Sprintf("%T", v),
					Hint: "could not get driver.Valuer value",
				}
			}
			return col.Append(val)
		}

		return nil, &ColumnConverterError{
			Op:   "Append",
			To:   "UInt32",
			From: fmt.Sprintf("%T", v),
		}
	}
	return
}

func (col *UInt32) AppendRow(v any) error {
	switch v := v.(type) {
	case uint32:
		col.col.Append(v)
	case *uint32:
		switch {
		case v != nil:
			col.col.Append(*v)
		default:
			col.col.Append(0)
		}
	case nil:
		col.col.Append(0)
	default:

		if valuer, ok := v.(driver.Valuer); ok {
			val, err := valuer.Value()
			if err != nil {
				return &ColumnConverterError{
					Op:   "AppendRow",
					To:   "UInt32",
					From: fmt.Sprintf("%T", v),
					Hint: "could not get driver.Valuer value",
				}
			}
			return col.AppendRow(val)
		}

		if rv := reflect.ValueOf(v); rv.Kind() == col.ScanType().Kind() || rv.CanConvert(col.ScanType()) {
			col.col.Append(rv.Convert(col.ScanType()).Interface().(uint32))
		} else {
			return &ColumnConverterError{
				Op:   "AppendRow",
				To:   "UInt32",
				From: fmt.Sprintf("%T", v),
			}
		}
	}
	return nil
}

func (col *UInt32) Decode(reader *proto.Reader, rows int) error {
	return col.col.DecodeColumn(reader, rows)
}

func (col *UInt32) Encode(buffer *proto.Buffer) {
	col.col.EncodeColumn(buffer)
}

func (col *UInt64) Name() string {
	return col.name
}

func (col *UInt64) Type() Type {
	return "UInt64"
}

func (col *UInt64) ScanType() reflect.Type {
	return scanTypeUInt64
}

func (col *UInt64) Rows() int {
	return col.col.Rows()
}

func (col *UInt64) Reset() {
	col.col.Reset()
}

func (col *UInt64) ScanRow(dest any, row int) error {
	value := col.col.Row(row)
	switch d := dest.(type) {
	case *uint64:
		*d = value
	case **uint64:
		*d = new(uint64)
		**d = value
	default:
		if scan, ok := dest.(sql.Scanner); ok {
			return scan.Scan(value)
		}
		return &ColumnConverterError{
			Op:   "ScanRow",
			To:   fmt.Sprintf("%T", dest),
			From: "UInt64",
			Hint: fmt.Sprintf("try using *%s", scanTypeUInt64),
		}
	}
	return nil
}

func (col *UInt64) Row(i int, ptr bool) any {
	value := col.col.Row(i)
	if ptr {
		return &value
	}
	return value
}

func (col *UInt64) Append(v any) (nulls []uint8, err error) {
	switch v := v.(type) {
	case []uint64:
		nulls = make([]uint8, len(v))
		for i := range v {
			col.col.Append(v[i])
		}
	case []*uint64:
		nulls = make([]uint8, len(v))
		for i := range v {
			switch {
			case v[i] != nil:
				col.col.Append(*v[i])
			default:
				col.col.Append(0)
				nulls[i] = 1
			}
		}
	default:

		if valuer, ok := v.(driver.Valuer); ok {
			val, err := valuer.Value()
			if err != nil {
				return nil, &ColumnConverterError{
					Op:   "Append",
					To:   "UInt64",
					From: fmt.Sprintf("%T", v),
					Hint: "could not get driver.Valuer value",
				}
			}
			return col.Append(val)
		}

		return nil, &ColumnConverterError{
			Op:   "Append",
			To:   "UInt64",
			From: fmt.Sprintf("%T", v),
		}
	}
	return
}

func (col *UInt64) AppendRow(v any) error {
	switch v := v.(type) {
	case uint64:
		col.col.Append(v)
	case *uint64:
		switch {
		case v != nil:
			col.col.Append(*v)
		default:
			col.col.Append(0)
		}
	case nil:
		col.col.Append(0)
	default:

		if valuer, ok := v.(driver.Valuer); ok {
			val, err := valuer.Value()
			if err != nil {
				return &ColumnConverterError{
					Op:   "AppendRow",
					To:   "UInt64",
					From: fmt.Sprintf("%T", v),
					Hint: "could not get driver.Valuer value",
				}
			}
			return col.AppendRow(val)
		}

		if rv := reflect.ValueOf(v); rv.Kind() == col.ScanType().Kind() || rv.CanConvert(col.ScanType()) {
			col.col.Append(rv.Convert(col.ScanType()).Interface().(uint64))
		} else {
			return &ColumnConverterError{
				Op:   "AppendRow",
				To:   "UInt64",
				From: fmt.Sprintf("%T", v),
			}
		}
	}
	return nil
}

func (col *UInt64) Decode(reader *proto.Reader, rows int) error {
	return col.col.DecodeColumn(reader, rows)
}

func (col *UInt64) Encode(buffer *proto.Buffer) {
	col.col.EncodeColumn(buffer)
}
