// Package app api・relay・consumer の3つのプロセスを組み立てる。main はここを呼ぶだけにする。
package app

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/okamyuji/event-streaming-valkey/internal/api"
	"github.com/okamyuji/event-streaming-valkey/internal/config"
	"github.com/okamyuji/event-streaming-valkey/internal/consumer"
	"github.com/okamyuji/event-streaming-valkey/internal/event"
	"github.com/okamyuji/event-streaming-valkey/internal/mysqlstore"
	"github.com/okamyuji/event-streaming-valkey/internal/outbox"
	"github.com/okamyuji/event-streaming-valkey/internal/platform"
	"github.com/okamyuji/event-streaming-valkey/internal/projection"
	"github.com/okamyuji/event-streaming-valkey/internal/stream"
	"github.com/valkey-io/valkey-go"
)

// RunFunc ctx が終わるまで動くプロセス本体。
type RunFunc func(ctx context.Context, cfg config.Config) error

// Main 設定を読み、SIGTERM で止まる ctx で run を動かして終了コードを返す。
func Main(name string, run RunFunc, getenv func(string) string) int {
	cfg, err := config.Load(getenv)
	if err != nil {
		slog.Error("設定が正しくない", "process", name, "err", err)
		return 2
	}
	ctx, stop := platform.SignalContext()
	defer stop()
	slog.Info("起動した", "process", name)
	if err := run(ctx, cfg); err != nil {
		slog.Error("停止した", "process", name, "err", err)
		return 1
	}
	return 0
}

// API 注文の受け付けと集計の読み出しを HTTP で提供する。
func API(ctx context.Context, cfg config.Config) error {
	db, err := platform.OpenMySQL(ctx, cfg.MySQLDSN)
	if err != nil {
		return err
	}
	defer func() { _ = db.Close() }()
	srv := &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           api.Handler{Store: mysqlstore.New(db)}.Routes(),
		ReadHeaderTimeout: cfg.ReadHeaderTimeout,
	}
	failed := make(chan error, 1)
	go func() { failed <- srv.ListenAndServe() }()
	select {
	case err := <-failed:
		return fmt.Errorf("待ち受けに失敗: %w", err)
	case <-ctx.Done():
	}
	shutdown, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	defer cancel()
	return srv.Shutdown(shutdown)
}

// Relay outbox の未送信イベントを Valkey Streams へ送る。順序を保つため1つだけ動かす。
func Relay(ctx context.Context, cfg config.Config) error {
	db, vk, err := open(ctx, cfg)
	if err != nil {
		return err
	}
	defer closeAll(db, vk)
	r := &outbox.Relay{
		Outbox:     mysqlstore.New(db),
		Publisher:  stream.NewValkey(vk, cfg.MaxLen),
		Topic:      cfg.Topic,
		Partitions: cfg.Partitions,
		Batch:      100,
		Interval:   200 * time.Millisecond,
		Log:        slog.Default(),
	}
	// Relay.Run は ctx が終わったときだけ戻るので、戻り値は停止の合図そのもの。
	_ = r.Run(ctx)
	return nil
}

// Consumer 1つのコンシューマグループとして全パーティションを読み、読み手の集計を更新する。
func Consumer(ctx context.Context, cfg config.Config) error {
	db, vk, err := open(ctx, cfg)
	if err != nil {
		return err
	}
	defer closeAll(db, vk)
	store := mysqlstore.New(db)
	var h consumer.Handler = projection.Inventory{Store: store, Group: cfg.Group}
	if cfg.Handler == "sales" {
		h = projection.Sales{Store: store, Projection: cfg.Group}
	}
	return consumer.RunAll(ctx, workers(cfg, stream.NewValkey(vk, cfg.MaxLen), h))
}

// workers パーティションごとに1つ Worker を作る。1つのパーティションを読むのは常に1つなので、注文ごとの順序が保たれる。
func workers(cfg config.Config, s consumer.Stream, h consumer.Handler) []consumer.Runner {
	runners := make([]consumer.Runner, 0, cfg.Partitions)
	for p := range cfg.Partitions {
		runners = append(runners, &consumer.Worker{
			Stream:  s,
			Handler: h,
			Config: consumer.Config{
				Stream:      event.StreamKey(cfg.Topic, p),
				Group:       cfg.Group,
				Consumer:    fmt.Sprintf("%s-p%d", cfg.Consumer, p),
				DLQ:         cfg.Topic + ".dlq",
				Start:       cfg.Start,
				MaxAttempts: cfg.MaxAttempts,
				RetryDelay:  cfg.RetryDelay,
				Block:       cfg.ReadBlock,
				Count:       10,
			},
			Log: slog.Default().With("group", cfg.Group, "partition", p),
		})
	}
	return runners
}

func open(ctx context.Context, cfg config.Config) (*sql.DB, valkey.Client, error) {
	db, err := platform.OpenMySQL(ctx, cfg.MySQLDSN)
	if err != nil {
		return nil, nil, err
	}
	vk, err := platform.OpenValkey(cfg.ValkeyAddr)
	if err != nil {
		_ = db.Close()
		return nil, nil, err
	}
	return db, vk, nil
}

func closeAll(db *sql.DB, vk valkey.Client) {
	vk.Close()
	_ = db.Close()
}
