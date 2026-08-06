package schemer

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/apis/swversion"
)

func TestQuoteIdentDoublesQuotes(t *testing.T) {
	if got := quoteIdent(`my"db`); got != `my""db` {
		t.Fatalf("quoteIdent must double embedded quotes; got %q", got)
	}
}

func TestSQLReplicaHealthShape(t *testing.T) {
	schemer := &ClusterSchemer{}
	sql := schemer.sqlReplicaHealth("is_readonly")
	if !strings.Contains(sql, "coalesce(max(is_readonly),0)") || !strings.Contains(sql, "system.replicas") {
		t.Fatalf("health SQL wrong: %s", sql)
	}
}

func TestHostMaxReplicaDelayReturnsCanceledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	delay, err := (&ClusterSchemer{}).HostMaxReplicaDelay(ctx, &api.Host{})
	if delay != 0 || !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled context must be returned; delay=%d err=%v", delay, err)
	}
}

func TestSQLSyncReplicaLightweight(t *testing.T) {
	schemer := &ClusterSchemer{}
	sql := schemer.sqlSyncReplicaLightweight(`my"db`, "tbl")
	if !strings.HasSuffix(sql, "LIGHTWEIGHT") {
		t.Fatalf("table sync must end with LIGHTWEIGHT: %s", sql)
	}
	if !strings.Contains(sql, `"my""db"."tbl"`) {
		t.Fatalf("identifiers must be quoted and escaped: %s", sql)
	}
}

func TestSQLSyncDatabaseReplicaHasNoLightweight(t *testing.T) {
	schemer := &ClusterSchemer{}
	sql := schemer.sqlSyncDatabaseReplica("db")
	if strings.Contains(sql, "LIGHTWEIGHT") {
		t.Fatalf("DATABASE REPLICA takes no LIGHTWEIGHT modifier: %s", sql)
	}
	if !strings.Contains(sql, "SYSTEM SYNC DATABASE REPLICA") || !strings.Contains(sql, `"db"`) {
		t.Fatalf("wrong DB-sync stmt: %s", sql)
	}
}

func TestSQLWaitLoadingPartsShape(t *testing.T) {
	schemer := &ClusterSchemer{}
	sql := schemer.sqlWaitLoadingParts("db", "tbl")
	if !strings.Contains(sql, "SYSTEM WAIT LOADING PARTS") || !strings.Contains(sql, `"db"."tbl"`) {
		t.Fatalf("wrong wait-loading-parts stmt: %s", sql)
	}
}

func TestSQLWithReceiveTimeoutCeilsRemainingSeconds(t *testing.T) {
	sql := sqlWithReceiveTimeout("SYSTEM SYNC REPLICA \"db\".\"tbl\" LIGHTWEIGHT", 1500*time.Millisecond)
	if !strings.HasSuffix(sql, "SETTINGS receive_timeout=2") {
		t.Fatalf("receive_timeout must ceil seconds: %s", sql)
	}
}

func TestSQLAsyncLoaderStateShape(t *testing.T) {
	schemer := &ClusterSchemer{}
	sql := schemer.sqlAsyncLoaderState()
	if !strings.Contains(sql, "countIf(status = 'PENDING'") || !strings.Contains(sql, "status IN ('FAILED', 'CANCELED')") {
		t.Fatalf("async loader state SQL must count pending and failed jobs: %s", sql)
	}
	if !strings.Contains(sql, "startsWith(job, 'startup ')") || !strings.Contains(sql, " database ") {
		t.Fatalf("async loader state SQL must filter relevant startup load jobs: %s", sql)
	}
}

func TestHostSyncReplicatedObjectsRejectsUnsupportedLightweightVersion(t *testing.T) {
	schemer := &ClusterSchemer{version: swversion.NewSoftWareVersion("23.3.22")}
	err := schemer.HostSyncReplicatedObjects(context.Background(), &api.Host{}, time.Now().Add(time.Minute))
	if err == nil {
		t.Fatalf("expected unsupported LIGHTWEIGHT error")
	}
	if !strings.Contains(err.Error(), "requires ClickHouse >= 23.4") {
		t.Fatalf("wrong version error: %v", err)
	}
}

func TestHostAsyncLoadBarrierReturnsGateDeadlineWhenExpired(t *testing.T) {
	schemer := &ClusterSchemer{}
	err := schemer.HostAsyncLoadBarrier(context.Background(), &api.Host{}, time.Now().Add(-time.Second))
	if !errors.Is(err, ErrGateDeadline) {
		t.Fatalf("expected ErrGateDeadline, got %v", err)
	}
}

func TestPeerReplicatedObjectCountReturnsGateDeadlineWhenExpired(t *testing.T) {
	schemer := &ClusterSchemer{}
	_, err := schemer.PeerReplicatedObjectCount(context.Background(), &api.Host{}, time.Now().Add(-time.Second))
	if !errors.Is(err, ErrGateDeadline) {
		t.Fatalf("expected ErrGateDeadline, got %v", err)
	}
}
