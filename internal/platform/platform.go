// Package platform 3つのプロセスが共通で使う、接続とシグナル処理の準備をまとめる。
package platform

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	// database/sql に MySQL ドライバを登録する。
	_ "github.com/go-sql-driver/mysql"
	"github.com/valkey-io/valkey-go"
)

// SignalContext SIGINT か SIGTERM で終わる context を返す。
func SignalContext() (context.Context, context.CancelFunc) {
	return signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
}

// OpenMySQL 接続を開き、実際につながることを確かめてから返す。
func OpenMySQL(ctx context.Context, dsn string) (*sql.DB, error) {
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, err
	}
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("接続できない (MySQL): %w", err)
	}
	return db, nil
}

// OpenValkey addr の Valkey につながるクライアントを返す。
func OpenValkey(addr string) (valkey.Client, error) {
	c, err := valkey.NewClient(valkey.ClientOption{InitAddress: []string{addr}})
	if err != nil {
		return nil, fmt.Errorf("接続できない (Valkey): %w", err)
	}
	return c, nil
}
