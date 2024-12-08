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
