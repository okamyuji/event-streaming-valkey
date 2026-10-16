package platform_test

import (
	"net"
	"syscall"
	"testing"
	"time"

	"github.com/okamyuji/event-streaming-valkey/internal/platform"
)

// closedAddr 何も待ち受けていないことが確かなアドレスを返す。
func closedAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := l.Addr().String()
	_ = l.Close()
	return addr
}

func TestOpenMySQLはつながらない相手にエラーを返す(t *testing.T) {
	db, err := platform.OpenMySQL(t.Context(), "root@tcp("+closedAddr(t)+")/events?timeout=1s")

	if err == nil || db != nil {
		t.Fatalf("エラーのはずが db=%v err=%v", db, err)
	}
}

func TestOpenMySQLは読めないDSNにエラーを返す(t *testing.T) {
	db, err := platform.OpenMySQL(t.Context(), "not a dsn")

	if err == nil || db != nil {
		t.Fatalf("エラーのはずが db=%v err=%v", db, err)
	}
}

func TestOpenValkeyはつながらない相手にエラーを返す(t *testing.T) {
	c, err := platform.OpenValkey(closedAddr(t))

	if err == nil || c != nil {
		t.Fatalf("エラーのはずが c=%v err=%v", c, err)
	}
}

func TestSignalContextはSIGTERMで終わる(t *testing.T) {
	ctx, stop := platform.SignalContext()
	defer stop()

	if err := syscall.Kill(syscall.Getpid(), syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}

	select {
	case <-ctx.Done():
	case <-time.After(time.Second):
		t.Fatal("SIGTERM で終わらなかった")
	}
}
