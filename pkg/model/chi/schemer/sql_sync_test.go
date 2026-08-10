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
	sql := schemer.sqlSyncReplica(`my"db`, "tbl", true)
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

// SYSTEM statements have no SETTINGS production - appending one is a parse-time SYNTAX_ERROR (Code 62).
func TestSQLSyncStatementsCarryNoSettingsClause(t *testing.T) {
	schemer := &ClusterSchemer{}
	for _, sql := range []string{
		schemer.sqlSyncReplica("db", "tbl", true),
		schemer.sqlSyncReplica("db", "tbl", false),
		schemer.sqlSyncDatabaseReplica("db"),
		schemer.sqlWaitLoadingParts("db", "tbl"),
	} {
		if strings.Contains(sql, "SETTINGS") {
			t.Fatalf("SYSTEM statement must carry no SETTINGS clause: %s", sql)
		}
	}
}

func TestSQLSyncReplicaLightweightToggle(t *testing.T) {
	schemer := &ClusterSchemer{}
	if !strings.HasSuffix(schemer.sqlSyncReplica("db", "tbl", true), "LIGHTWEIGHT") {
		t.Fatalf("lightweight variant must end with LIGHTWEIGHT")
	}
	if strings.Contains(schemer.sqlSyncReplica("db", "tbl", false), "LIGHTWEIGHT") {
		t.Fatalf("fallback variant must not use LIGHTWEIGHT")
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

// An unknown or pre-23.4 version must NOT fail the gate - it falls back to full SYNC REPLICA.
func TestHostSyncReplicatedObjectsFailsOpenOnOldVersion(t *testing.T) {
	for _, version := range []string{"23.3.22", "0.0.1"} {
		schemer := &ClusterSchemer{version: swversion.NewSoftWareVersion(version)}
		err := schemer.HostSyncReplicatedObjects(context.Background(), &api.Host{}, time.Now().Add(-time.Second))
		// The version decision is taken before the async-load barrier, so an expired deadline
		// proves the gate got past it: a hard-fail would surface the version error here instead
		// of ErrGateDeadline.
		if !errors.Is(err, ErrGateDeadline) {
			t.Fatalf("version %s must not hard-fail the gate, want ErrGateDeadline, got %v", version, err)
		}
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
