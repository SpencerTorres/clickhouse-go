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

package tests

import (
	"context"
	"encoding/json"
	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/chcol"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestJSON(t *testing.T) {
	ctx := context.Background()

	conn, err := GetNativeConnection(clickhouse.Settings{
		"max_execution_time":           60,
		"allow_experimental_json_type": true,
	}, nil, &clickhouse.Compression{
		Method: clickhouse.CompressionLZ4,
	})
	require.NoError(t, err)

	const ddl = `
			CREATE TABLE IF NOT EXISTS test_json (
				  c JSON
			) Engine = MergeTree() ORDER BY tuple()
		`
	require.NoError(t, conn.Exec(ctx, ddl))
	defer func() {
		require.NoError(t, conn.Exec(ctx, "DROP TABLE IF EXISTS test_json"))
	}()

	batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_json (c)")
	require.NoError(t, err)

	jsonRow := chcol.NewJSON()
	jsonRow.SetValueAtPath("a.a", chcol.NewDynamic(true).WithType("Bool"))
	jsonRow.SetValueAtPath("a.b", chcol.NewDynamic(42).WithType("Int64"))
	jsonRow.SetValueAtPath("a.c", chcol.NewDynamic("test!").WithType("String"))
	jsonRow.SetValueAtPath("x", chcol.NewDynamic(64).WithType("Int64"))
	require.NoError(t, batch.Append(jsonRow))

	require.NoError(t, batch.Send())

	rows, err := conn.Query(ctx, "SELECT c FROM test_json")
	require.NoError(t, err)

	var row chcol.JSON

	require.True(t, rows.Next())
	err = rows.Scan(&row)
	require.NoError(t, err)

	aaDynamic, ok := row.ValueAtPath("a.a")
	require.True(t, ok)
	aa := aaDynamic.(chcol.Dynamic)
	require.Equal(t, true, aa.MustBool())

	abDynamic, ok := row.ValueAtPath("a.b")
	require.True(t, ok)
	ab := abDynamic.(chcol.Dynamic)
	require.Equal(t, int64(42), ab.MustInt64())

	acDynamic, ok := row.ValueAtPath("a.c")
	require.True(t, ok)
	ac := acDynamic.(chcol.Dynamic)
	require.Equal(t, "test!", ac.MustString())

	xDynamic, ok := row.ValueAtPath("x")
	require.True(t, ok)
	x := xDynamic.(chcol.Dynamic)
	require.Equal(t, int64(64), x.MustInt64())

}

type Address struct {
	Street  string `chType:"String"`
	City    string `chType:"String"`
	Country string `chType:"String"`
}

type TestStruct struct {
	Name   string  `chType:"String"`
	Age    int64   `chType:"Int64"`
	Active bool    `chType:"Bool"`
	Score  float64 `chType:"Float64"`

	Tags    []string `chType:"Array(String)"`
	Numbers []int64  `chType:"Array(Int64)"`

	Address Address

	KeysNumbers map[string]int64 `chType:"Map(String, Int64)"`
	Metadata    map[string]interface{}

	DynamicString chcol.DynamicWithType
	DynamicInt    chcol.DynamicWithType
	DynamicMap    chcol.DynamicWithType
}

func TestJSONStruct(t *testing.T) {
	ctx := context.Background()

	conn, err := GetNativeConnection(clickhouse.Settings{
		"max_execution_time":           60,
		"allow_experimental_json_type": true,
	}, nil, &clickhouse.Compression{
		Method: clickhouse.CompressionLZ4,
	})
	require.NoError(t, err)

	const ddl = `
			CREATE TABLE IF NOT EXISTS test_json (
				  c JSON(Name String, Age Int64, KeysNumbers Map(String, Int64), SKIP fake.field)
			) Engine = MergeTree() ORDER BY tuple()
		`
	require.NoError(t, conn.Exec(ctx, ddl))
	defer func() {
		require.NoError(t, conn.Exec(ctx, "DROP TABLE IF EXISTS test_json"))
	}()

	batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_json (c)")
	require.NoError(t, err)

	//jsonRow := chcol.NewJSON()
	//jsonRow.SetValueAtPath("Name", chcol.NewDynamic("Mr. JSON").WithType("String"))
	//jsonRow.SetValueAtPath("Age", chcol.NewDynamic(42).WithType("Int64"))
	//jsonRow.SetValueAtPath("Active", chcol.NewDynamic(true).WithType("Bool"))
	//jsonRow.SetValueAtPath("Score", chcol.NewDynamic(3.14).WithType("Float64"))
	//jsonRow.SetValueAtPath("Tags", chcol.NewDynamic([]string{"a", "b"}).WithType("Array(String)"))
	//jsonRow.SetValueAtPath("Numbers", chcol.NewDynamic([]int{20, 40}).WithType("Array(Int64)"))
	//jsonRow.SetValueAtPath("Address.Street", chcol.NewDynamic("2024 JSON St").WithType("String"))
	//jsonRow.SetValueAtPath("Address.City", chcol.NewDynamic("JSON City").WithType("String"))
	//jsonRow.SetValueAtPath("Address.Country", chcol.NewDynamic("JSON World").WithType("String"))
	//jsonRow.SetValueAtPath("KeysNumbers", chcol.NewDynamic(map[string]int64{"FieldA": 42, "FieldB": 32}).WithType("Map(String, Int64)"))
	//jsonRow.SetValueAtPath("Metadata.FieldA", chcol.NewDynamic("a").WithType("String"))
	//jsonRow.SetValueAtPath("Metadata.FieldB", chcol.NewDynamic("b").WithType("String"))
	//jsonRow.SetValueAtPath("Metadata.FieldC.FieldD", chcol.NewDynamic("d").WithType("String"))
	//jsonRow.SetValueAtPath("DynamicString", chcol.NewDynamic("str").WithType("String"))
	//jsonRow.SetValueAtPath("DynamicInt", chcol.NewDynamic(48).WithType("Int64"))
	//jsonRow.SetValueAtPath("DynamicMap", chcol.NewDynamic(map[string]string{"a": "a", "b": "b"}).WithType("Map(String, String)"))
	inputRow := TestStruct{
		Name:    "Mr. JSON",
		Age:     42,
		Active:  true,
		Score:   3.14,
		Tags:    []string{"a", "b"},
		Numbers: []int64{20, 40},
		Address: Address{
			Street:  "2024 JSON St",
			City:    "JSON City",
			Country: "JSON World",
		},
		KeysNumbers: map[string]int64{"FieldA": 42, "FieldB": 32},
		//Metadata: map[string]interface{}{
		//	"FieldA": "a",
		//	"FieldB": "b",
		//	"FieldC": map[string]interface{}{
		//		"FieldD": "d",
		//	},
		//},
		DynamicString: chcol.NewDynamic("str").WithType("String"),
		DynamicInt:    chcol.NewDynamic(48).WithType("Int64"),
		DynamicMap:    chcol.NewDynamic(map[string]string{"a": "a", "b": "b"}).WithType("Map(String, String)"),
	}
	require.NoError(t, batch.Append(inputRow))

	require.NoError(t, batch.Send())

	rows, err := conn.Query(ctx, "SELECT c FROM test_json")
	require.NoError(t, err)

	var row TestStruct

	require.True(t, rows.Next())
	err = rows.Scan(&row)
	require.NoError(t, err)
}

func TestJSONString(t *testing.T) {
	ctx := context.Background()

	conn, err := GetNativeConnection(clickhouse.Settings{
		"max_execution_time":                        60,
		"allow_experimental_json_type":              true,
		"output_format_native_write_json_as_string": true,
	}, nil, &clickhouse.Compression{
		Method: clickhouse.CompressionLZ4,
	})
	require.NoError(t, err)

	const ddl = `
			CREATE TABLE IF NOT EXISTS test_json (
				  c JSON(Name String, Age Int64, KeysNumbers Map(String, Int64), SKIP fake.field)
			) Engine = MergeTree() ORDER BY tuple()
		`
	require.NoError(t, conn.Exec(ctx, ddl))
	defer func() {
		require.NoError(t, conn.Exec(ctx, "DROP TABLE IF EXISTS test_json"))
	}()

	batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_json (c)")
	require.NoError(t, err)

	inputRow := TestStruct{
		Name:    "Mr. JSON",
		Age:     42,
		Active:  true,
		Score:   3.14,
		Tags:    []string{"a", "b"},
		Numbers: []int64{20, 40},
		Address: Address{
			Street:  "2024 JSON St",
			City:    "JSON City",
			Country: "JSON World",
		},
		KeysNumbers: map[string]int64{"FieldA": 42, "FieldB": 32},
		//Metadata: map[string]interface{}{
		//	"FieldA": "a",
		//	"FieldB": "b",
		//	"FieldC": map[string]interface{}{
		//		"FieldD": "d",
		//	},
		//},
		DynamicString: chcol.NewDynamic("str").WithType("String"),
		DynamicInt:    chcol.NewDynamic(48).WithType("Int64"),
		DynamicMap:    chcol.NewDynamic(map[string]string{"a": "a", "b": "b"}).WithType("Map(String, String)"),
	}

	inputRowStr, err := json.Marshal(inputRow)
	require.NoError(t, err)
	require.NoError(t, batch.Append(inputRowStr))

	require.NoError(t, batch.Send())

	rows, err := conn.Query(ctx, "SELECT c FROM test_json")
	require.NoError(t, err)

	var row json.RawMessage

	require.True(t, rows.Next())
	err = rows.Scan(&row)
	require.NoError(t, err)

	var rowStruct TestStruct
	err = json.Unmarshal(row, &rowStruct)
	require.NoError(t, err)
}
