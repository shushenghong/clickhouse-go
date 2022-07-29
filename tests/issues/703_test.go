package issues

import (
	"context"
	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
)

func Test703(t *testing.T) {
	var (
		ctx       = context.Background()
		conn, err = clickhouse.Open(&clickhouse.Options{
			Addr: []string{"127.0.0.1:9000"},
			Auth: clickhouse.Auth{
				Database: "default",
				Username: "default",
				Password: "",
			},
		})
	)
	require.NoError(t, err)
	conn.Exec(ctx, "DROP TABLE test_enum")
	const ddl = `CREATE TABLE test_enum (
				Col1 Enum8 ('Click'=5, 'House'=25)
			) Engine Memory`
	require.NoError(t, conn.Exec(ctx, ddl))

	defer func() {
		conn.Exec(ctx, "DROP TABLE test_enum")
	}()
	batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_enum")
	require.NoError(t, err)
	type request struct {
		Col1 string
	}
	assert.NoError(t, batch.AppendStruct(&request{
		Col1: "house",
	}))
	require.NoError(t, batch.Send())
}
