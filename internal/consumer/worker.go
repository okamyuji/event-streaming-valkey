// Package consumer コンシューマグループの一員としてパーティションを読み、処理結果に応じて ACK・再試行・退避を決める。
package consumer

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/okamyuji/event-streaming-valkey/internal/event"
	"github.com/okamyuji/event-streaming-valkey/internal/stream"
)

// Stream Worker が使うストリーム操作。本番では stream.Valkey が満たす。
type Stream interface {
	EnsureGroup(ctx context.Context, stream, group, start string) error
	Read(ctx context.Context, stream, group, consumer, id string, count int64, block time.Duration) ([]stream.Message, error)
	Claim(ctx context.Context, stream, group, consumer string, minIdle time.Duration, count int64) ([]stream.Message, error)
	Ack(ctx context.Context, stream, group, id string) error
	DeadLetter(ctx context.Context, dlq, source string, m stream.Message, reason string) error
}

// Handler イベント1件を処理する。同じイベントが2回以上届いても結果が変わらないように作る。
type Handler interface {
	Handle(ctx context.Context, e event.Envelope) error
}

// Action 処理結果を受けて Worker が取る動き。
type Action int

const (
	// Ack 処理済みとして未ACK一覧から外す。
	Ack Action = iota
	// Retry 少し待って同じイベントをもう一度処理する。その間、後ろのイベントには進まない。
	Retry
	// DeadLetter 退避用ストリームへ移してから ACK し、後ろのイベントへ進む。
	DeadLetter
)

func (a Action) String() string {
	return [...]string{"Ack", "Retry", "DeadLetter"}[a]
}

// Decide 処理のエラーと何回目の試行かから次の動きを決める。
func Decide(err error, attempt, maxAttempts int) Action {
	if err == nil {
		return Ack
	}
	if errors.Is(err, event.ErrMalformed) || attempt >= maxAttempts {
		return DeadLetter
	}
	return Retry
}

// Config 1つのパーティションを読む Worker の設定。
type Config struct {
	Stream   string
	Group    string
	Consumer string
	DLQ      string
	// Start グループを新しく作るときの開始位置。"0" なら残っている最古から、"$" なら今後の新着だけを読む。
	Start       string
	MaxAttempts int
	RetryDelay  time.Duration
	Block       time.Duration
	Count       int64
}

// Worker 1つのパーティションを、前のイベントを片付けてから次へ進む順で読み続ける。
// 順序を守るため、1つのパーティションをグループ内で同時に読む Worker は1つだけにする。
type Worker struct {
	Stream  Stream
	Handler Handler
	Config  Config
	Log     *slog.Logger
}

// Run ctx が終わるか、ストリームの操作に失敗するまで読み続ける。
func (w *Worker) Run(ctx context.Context) error {
	c := w.Config
	if err := w.Stream.EnsureGroup(ctx, c.Stream, c.Group, c.Start); err != nil {
		return fmt.Errorf("グループ作成: %w", err)
	}
	if err := w.recoverPending(ctx); err != nil {
		return err
	}
	for {
		msgs, err := w.Stream.Read(ctx, c.Stream, c.Group, c.Consumer, ">", c.Count, c.Block)
		if err != nil {
			return stopOrErr(ctx, fmt.Errorf("読み込み: %w", err))
		}
		if err := w.processAll(ctx, msgs); err != nil {
			return err
		}
		if err := ctx.Err(); err != nil {
			return err
		}
	}
}

// recoverPending 前の持ち主が ACK しないまま止まった分を放置時間を待たずにすべて引き取り、新着より先に古い順で片付ける。
func (w *Worker) recoverPending(ctx context.Context) error {
	c := w.Config
	for {
		msgs, err := w.Stream.Claim(ctx, c.Stream, c.Group, c.Consumer, 0, c.Count)
		if err != nil {
			return stopOrErr(ctx, fmt.Errorf("未ACK分の引き取り: %w", err))
		}
		if len(msgs) == 0 {
			return nil
		}
		if err := w.processAll(ctx, msgs); err != nil {
			return err
		}
	}
}

func (w *Worker) processAll(ctx context.Context, msgs []stream.Message) error {
	for _, m := range msgs {
		if err := w.process(ctx, m); err != nil {
			return err
		}
	}
	return nil
}

// process 1件を片付ける。成功するか退避するまで戻らないので、後ろのイベントが先に処理されることはない。
func (w *Worker) process(ctx context.Context, m stream.Message) error {
	c := w.Config
	err := w.handle(ctx, m)
	if ctxErr := ctx.Err(); ctxErr != nil {
		// 未ACKのまま止まり、次に起動した Worker が引き取ってやり直す。
		return ctxErr
	}
	if err != nil {
		w.Log.Error("DLQへ退避する", "stream", c.Stream, "id", m.ID, "err", err)
		if derr := w.Stream.DeadLetter(ctx, c.DLQ, c.Stream, m, err.Error()); derr != nil {
			return fmt.Errorf("DLQへの書き込み: %w", derr)
		}
	}
	if aerr := w.Stream.Ack(ctx, c.Stream, c.Group, m.ID); aerr != nil {
		return fmt.Errorf("ACK: %w", aerr)
	}
	return nil
}

// handle 再試行の上限まで同じイベントを処理し直す。諦めたときだけ最後のエラーを返す。
func (w *Worker) handle(ctx context.Context, m stream.Message) error {
	c := w.Config
	env, err := event.FromFields(m.Fields)
	if err != nil {
		return err
	}
	for attempt := 1; ; attempt++ {
		err := w.Handler.Handle(ctx, env)
		if Decide(err, attempt, c.MaxAttempts) != Retry {
			return err
		}
		w.Log.Warn("処理に失敗したので待ってやり直す", "stream", c.Stream, "id", m.ID, "attempt", attempt, "err", err)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(c.RetryDelay):
		}
	}
}

// stopOrErr 停止の合図で操作が中断されたときは、操作の失敗ではなく停止として返す。
func stopOrErr(ctx context.Context, err error) error {
	if ctxErr := ctx.Err(); ctxErr != nil {
		return ctxErr
	}
	return err
}
