package consumer_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"slices"
	"testing"
	"time"

	"github.com/okamyuji/event-streaming-valkey/internal/consumer"
	"github.com/okamyuji/event-streaming-valkey/internal/event"
	"github.com/okamyuji/event-streaming-valkey/internal/stream"
)

var errTemporary = errors.New("DBに一時的につながらない")

func TestDecideは結果と試行回数から次の動きを決める(t *testing.T) {
	cases := []struct {
		name    string
		err     error
		attempt int
		want    consumer.Action
	}{
		{"成功ならACK", nil, 1, consumer.Ack},
		{"壊れたイベントは初回でもDLQ", fmt.Errorf("x: %w", event.ErrMalformed), 1, consumer.DeadLetter},
		{"一時的な失敗は上限未満なら再試行", errTemporary, 2, consumer.Retry},
		{"一時的な失敗が上限に達したらDLQ", errTemporary, 3, consumer.DeadLetter},
		{"一時的な失敗が上限を超えてもDLQ", errTemporary, 4, consumer.DeadLetter},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := consumer.Decide(c.err, c.attempt, 3); got != c.want {
				t.Fatalf("%v のはずが %v", c.want, got)
			}
		})
	}
}

func TestActionは名前を文字列で返す(t *testing.T) {
	got := []string{consumer.Ack.String(), consumer.Retry.String(), consumer.DeadLetter.String()}
	if !slices.Equal(got, []string{"Ack", "Retry", "DeadLetter"}) {
		t.Fatalf("名前が違う: %v", got)
	}
}

// fakeStream 1つのストリームと1つのグループだけを持つメモリ上の Stream。
type fakeStream struct {
	pending    []stream.Message
	fresh      []stream.Message
	acked      []string
	dead       []string
	groupStart string
	claimIdle  []time.Duration
	readErr    error
	claimErr   error
	ackErr     error
	deadErr    error
	cancel     context.CancelFunc
}

func (f *fakeStream) EnsureGroup(_ context.Context, _, _, start string) error {
	f.groupStart = start
	return nil
}

// Claim 実物と同じく、1回に count 件までを返し、止める合図が出ていればエラーを返す。
func (f *fakeStream) Claim(ctx context.Context, _, _, _ string, minIdle time.Duration, count int64) ([]stream.Message, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	f.claimIdle = append(f.claimIdle, minIdle)
	var out []stream.Message
	for _, m := range f.pending {
		if !slices.Contains(f.acked, m.ID) && int64(len(out)) < count {
			out = append(out, m)
		}
	}
	return out, f.claimErr
}

func (f *fakeStream) Read(context.Context, string, string, string, string, int64, time.Duration) ([]stream.Message, error) {
	if f.readErr != nil {
		return nil, f.readErr
	}
	if len(f.fresh) == 0 {
		f.cancel()
		return nil, nil
	}
	out := f.fresh
	f.fresh = nil
	return out, nil
}

func (f *fakeStream) Ack(_ context.Context, _, _, id string) error {
	if f.ackErr != nil {
		return f.ackErr
	}
	f.acked = append(f.acked, id)
	return nil
}

func (f *fakeStream) DeadLetter(_ context.Context, _, _ string, m stream.Message, _ string) error {
	if f.deadErr != nil {
		return f.deadErr
	}
	f.dead = append(f.dead, m.ID)
	return nil
}

// recorder 受け取ったイベントIDを順に記録し、fail が返す失敗をそのまま返す Handler。
type recorder struct {
	handled []string
	fail    func(id string, nth int) error
}

func (r *recorder) Handle(_ context.Context, e event.Envelope) error {
	nth := 0
	for _, id := range r.handled {
		if id == e.ID {
			nth++
		}
	}
	r.handled = append(r.handled, e.ID)
	if r.fail == nil {
		return nil
	}
	return r.fail(e.ID, nth+1)
}

func msg(id, eventID string) stream.Message {
	return stream.Message{ID: id, Fields: event.Envelope{
		ID: eventID, Type: event.TypeOrderPlaced, Key: "k", OccurredAt: time.Now(), Payload: []byte("{}"),
	}.Fields()}
}

func newWorker(s *fakeStream, h consumer.Handler, retryDelay time.Duration) *consumer.Worker {
	return &consumer.Worker{
		Stream:  s,
		Handler: h,
		Config: consumer.Config{
			Stream: "orders.p0", Group: "inventory", Consumer: "c1", DLQ: "orders.dlq",
			Start: "0", MaxAttempts: 3, RetryDelay: retryDelay, Block: time.Millisecond, Count: 1,
		},
		Log: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
}

func run(t *testing.T, s *fakeStream, h consumer.Handler) error {
	t.Helper()
	// 変異で無限ループになっても、時間切れではなく失敗として検出させる。
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	s.cancel = cancel
	return newWorker(s, h, time.Millisecond).Run(ctx)
}

func TestRunは指定した位置でグループを作り新着を処理してACKする(t *testing.T) {
	s := &fakeStream{fresh: []stream.Message{msg("1-0", "e1"), msg("2-0", "e2")}}
	h := &recorder{}

	err := run(t, s, h)

	if !errors.Is(err, context.Canceled) {
		t.Fatalf("キャンセルで終わるはずが %v", err)
	}
	if s.groupStart != "0" {
		t.Fatalf("開始位置 0 でグループを作るはずが %q", s.groupStart)
	}
	if !slices.Equal(h.handled, []string{"e1", "e2"}) || !slices.Equal(s.acked, []string{"1-0", "2-0"}) {
		t.Fatalf("処理=%v ACK=%v", h.handled, s.acked)
	}
}

func TestRunは起動時に未ACK分を放置時間を待たずに全部引き取り新着より先に古い順に処理する(t *testing.T) {
	s := &fakeStream{
		pending: []stream.Message{msg("1-0", "old1"), msg("2-0", "old2")},
		fresh:   []stream.Message{msg("3-0", "new")},
	}
	h := &recorder{}

	_ = run(t, s, h)

	if !slices.Equal(h.handled, []string{"old1", "old2", "new"}) {
		t.Fatalf("未ACK分→新着の順のはずが %v", h.handled)
	}
	if len(s.claimIdle) == 0 || slices.ContainsFunc(s.claimIdle, func(d time.Duration) bool { return d != 0 }) {
		t.Fatalf("放置時間0で引き取るはずが %v", s.claimIdle)
	}
}

func TestRunは一時的な失敗をその場で再試行し成功するまで次へ進まない(t *testing.T) {
	s := &fakeStream{fresh: []stream.Message{msg("1-0", "e1"), msg("2-0", "e2")}}
	h := &recorder{fail: func(id string, nth int) error {
		if id == "e1" && nth < 3 {
			return errTemporary
		}
		return nil
	}}

	_ = run(t, s, h)

	if !slices.Equal(h.handled, []string{"e1", "e1", "e1", "e2"}) {
		t.Fatalf("e1 を3回試してから e2 のはずが %v", h.handled)
	}
	if !slices.Equal(s.acked, []string{"1-0", "2-0"}) || len(s.dead) != 0 {
		t.Fatalf("ACK=%v DLQ=%v", s.acked, s.dead)
	}
}

func TestRunは再試行が上限に達したらDLQへ送ってから次へ進む(t *testing.T) {
	s := &fakeStream{fresh: []stream.Message{msg("1-0", "e1"), msg("2-0", "e2")}}
	h := &recorder{fail: func(id string, _ int) error {
		if id == "e1" {
			return errTemporary
		}
		return nil
	}}

	_ = run(t, s, h)

	if !slices.Equal(h.handled, []string{"e1", "e1", "e1", "e2"}) {
		t.Fatalf("上限の3回で諦めるはずが %v", h.handled)
	}
	if !slices.Equal(s.dead, []string{"1-0"}) || !slices.Equal(s.acked, []string{"1-0", "2-0"}) {
		t.Fatalf("DLQ=%v ACK=%v", s.dead, s.acked)
	}
}

func TestRunは壊れたエントリをハンドラに渡さずDLQへ送ってACKする(t *testing.T) {
	broken := stream.Message{ID: "1-0", Fields: map[string]string{"payload": "{"}}
	s := &fakeStream{fresh: []stream.Message{broken}}
	h := &recorder{}

	_ = run(t, s, h)

	if len(h.handled) != 0 {
		t.Fatalf("壊れたエントリがハンドラに渡った: %v", h.handled)
	}
	if !slices.Equal(s.dead, []string{"1-0"}) || !slices.Equal(s.acked, []string{"1-0"}) {
		t.Fatalf("DLQ=%v ACK=%v", s.dead, s.acked)
	}
}

func TestRunはハンドラが壊れていると判断したイベントを再試行せずDLQへ送る(t *testing.T) {
	s := &fakeStream{fresh: []stream.Message{msg("1-0", "e1")}}
	h := &recorder{fail: func(string, int) error { return fmt.Errorf("payload: %w", event.ErrMalformed) }}

	_ = run(t, s, h)

	if !slices.Equal(h.handled, []string{"e1"}) || !slices.Equal(s.dead, []string{"1-0"}) {
		t.Fatalf("1回で諦めるはずが 処理=%v DLQ=%v", h.handled, s.dead)
	}
}

func TestRunは再試行を待つ間に停止の合図が来たらACKせずに止まる(t *testing.T) {
	s := &fakeStream{fresh: []stream.Message{msg("1-0", "e1")}}
	ctx, cancel := context.WithCancel(t.Context())
	s.cancel = cancel
	h := &recorder{fail: func(string, int) error {
		cancel()
		return errTemporary
	}}

	err := newWorker(s, h, time.Hour).Run(ctx)

	if !errors.Is(err, context.Canceled) {
		t.Fatalf("停止で終わるはずが %v", err)
	}
	if len(s.acked) != 0 || len(s.dead) != 0 {
		t.Fatalf("未ACKのまま残すはずが ACK=%v DLQ=%v", s.acked, s.dead)
	}
}

func TestRunはストリーム操作の失敗を返して止まる(t *testing.T) {
	boom := errors.New("接続断")
	cases := map[string]*fakeStream{
		"読み込み": {readErr: boom},
		"引き取り": {claimErr: boom},
		"ACK":  {fresh: []stream.Message{msg("1-0", "e1")}, ackErr: boom},
		"DLQ":  {fresh: []stream.Message{{ID: "1-0"}}, deadErr: boom},
	}
	for name, s := range cases {
		t.Run(name, func(t *testing.T) {
			if err := run(t, s, &recorder{}); !errors.Is(err, boom) {
				t.Fatalf("失敗を返すはずが %v", err)
			}
		})
	}
}
