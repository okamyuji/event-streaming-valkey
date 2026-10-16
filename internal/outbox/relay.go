// Package outbox DBに書いたイベントを、DBの書き込みと切り離してストリームへ届ける。
package outbox

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/okamyuji/event-streaming-valkey/internal/event"
)

// Record outbox テーブルの1行。Seq は書き込まれた順を表す。
type Record struct {
	Seq      int64
	Envelope event.Envelope
}

// Store 未送信のイベントを取り出し、送信済みに印を付ける。本番では mysqlstore.Store が満たす。
type Store interface {
	Unpublished(ctx context.Context, limit int) ([]Record, error)
	MarkPublished(ctx context.Context, seq int64) error
}

// Publisher イベントをストリームへ追記する。本番では stream.Valkey が満たす。
type Publisher interface {
	Publish(ctx context.Context, stream string, e event.Envelope) (string, error)
}

// Relay outbox を定期的に見て、未送信のイベントをパーティションごとのストリームへ送る。
type Relay struct {
	Outbox     Store
	Publisher  Publisher
	Topic      string
	Partitions int
	Batch      int
	Interval   time.Duration
	Log        *slog.Logger
}

// Once 未送信分を古い順に送り、送れた件数を返す。途中で失敗したら残りは次回に回す。
func (r *Relay) Once(ctx context.Context) (int, error) {
	recs, err := r.Outbox.Unpublished(ctx, r.Batch)
	if err != nil {
		return 0, fmt.Errorf("未送信分の取り出し: %w", err)
	}
	for i, rec := range recs {
		key := event.StreamKey(r.Topic, event.Partition(rec.Envelope.Key, r.Partitions))
		if _, err := r.Publisher.Publish(ctx, key, rec.Envelope); err != nil {
			return i, fmt.Errorf("送信 seq=%d: %w", rec.Seq, err)
		}
		// ここで落ちると同じイベントを次回もう一度送る。読み手の重複排除が前提になる。
		if err := r.Outbox.MarkPublished(ctx, rec.Seq); err != nil {
			return i, fmt.Errorf("送信済みの記録 seq=%d: %w", rec.Seq, err)
		}
	}
	return len(recs), nil
}

// Run ctx が終わるまで Interval ごとに Once を繰り返す。失敗は記録して次の周期でやり直す。
func (r *Relay) Run(ctx context.Context) error {
	t := time.NewTicker(r.Interval)
	defer t.Stop()
	for {
		n, err := r.Once(ctx)
		if err != nil {
			r.Log.Warn("送信に失敗したので次の周期でやり直す", "err", err)
		} else if n > 0 {
			r.Log.Info("送信した", "count", n)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
		}
	}
}
