package outbox_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/okamyuji/event-streaming-valkey/internal/event"
	"github.com/okamyuji/event-streaming-valkey/internal/outbox"
)

type fakeOutbox struct {
	records   []outbox.Record
	published []int64
	limit     int
	fetchErr  error
	// failFetches 最初のこの回数だけ取り出しに失敗する。
	failFetches int
	markErr     error
}

func (f *fakeOutbox) Unpublished(_ context.Context, limit int) ([]outbox.Record, error) {
	f.limit = limit
	if f.failFetches > 0 {
		f.failFetches--
		return nil, errors.New("一時的な失敗")
	}
	var out []outbox.Record
	for _, r := range f.records {
		if !slices.Contains(f.published, r.Seq) {
			out = append(out, r)
		}
	}
	return out, f.fetchErr
}

func (f *fakeOutbox) MarkPublished(_ context.Context, seq int64) error {
	if f.markErr != nil {
		return f.markErr
	}
	f.published = append(f.published, seq)
	return nil
}

type sent struct {
	stream  string
	eventID string
}

type fakePublisher struct {
	sent   []sent
	failOn string
}

func (f *fakePublisher) Publish(_ context.Context, stream string, e event.Envelope) (string, error) {
	if e.ID == f.failOn {
		return "", errors.New("Valkeyにつながらない")
	}
	f.sent = append(f.sent, sent{stream, e.ID})
	return "1-0", nil
}

func record(seq int64, eventID, key string) outbox.Record {
	return outbox.Record{Seq: seq, Envelope: event.Envelope{ID: eventID, Key: key}}
}

func newRelay(o *fakeOutbox, p *fakePublisher) *outbox.Relay {
	return &outbox.Relay{
		Outbox: o, Publisher: p, Topic: "orders", Partitions: 3, Batch: 50,
		Interval: time.Millisecond, Log: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
}

func TestOnceは未送信分を古い順にキーのパーティションへ送り送信済みにする(t *testing.T) {
	o := &fakeOutbox{records: []outbox.Record{record(1, "e1", "order-a"), record(2, "e2", "order-b")}}
	p := &fakePublisher{}

	n, err := newRelay(o, p).Once(t.Context())

	if err != nil {
		t.Fatal(err)
	}
	want := []sent{
		{event.StreamKey("orders", event.Partition("order-a", 3)), "e1"},
		{event.StreamKey("orders", event.Partition("order-b", 3)), "e2"},
	}
	if n != 2 || !slices.Equal(p.sent, want) || !slices.Equal(o.published, []int64{1, 2}) {
		t.Fatalf("n=%d sent=%v published=%v", n, p.sent, o.published)
	}
	if o.limit != 50 {
		t.Fatalf("Batch の件数で取り出すはずが %d", o.limit)
	}
}

func TestOnceは送れなかった時点で止まりそれ以降を送信済みにしない(t *testing.T) {
	o := &fakeOutbox{records: []outbox.Record{record(1, "e1", "a"), record(2, "e2", "b"), record(3, "e3", "c")}}
	p := &fakePublisher{failOn: "e2"}

	n, err := newRelay(o, p).Once(t.Context())

	if err == nil {
		t.Fatal("送信の失敗を返すはず")
	}
	if n != 1 || !slices.Equal(o.published, []int64{1}) {
		t.Fatalf("n=%d published=%v", n, o.published)
	}
}

func TestOnceは取り出しの失敗を返す(t *testing.T) {
	o := &fakeOutbox{fetchErr: errors.New("MySQLにつながらない")}

	if _, err := newRelay(o, &fakePublisher{}).Once(t.Context()); err == nil {
		t.Fatal("取り出しの失敗を返すはず")
	}
}

func TestOnceは送信済みの記録に失敗したらそこで止まる(t *testing.T) {
	o := &fakeOutbox{records: []outbox.Record{record(1, "e1", "a"), record(2, "e2", "b")}, markErr: errors.New("更新失敗")}
	p := &fakePublisher{}

	n, err := newRelay(o, p).Once(t.Context())

	if err == nil || n != 0 || len(p.sent) != 1 {
		t.Fatalf("1件送った所で止まるはずが n=%d sent=%v err=%v", n, p.sent, err)
	}
}

func TestRunは停止の合図まで繰り返し送る(t *testing.T) {
	o := &fakeOutbox{records: []outbox.Record{record(1, "e1", "a")}}
	p := &fakePublisher{}
	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()

	err := newRelay(o, p).Run(ctx)

	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("停止の合図で終わるはずが %v", err)
	}
	if len(p.sent) != 1 {
		t.Fatalf("1回だけ送るはずが %v", p.sent)
	}
}

func TestRunは失敗しても止まらず次の周期でやり直す(t *testing.T) {
	o := &fakeOutbox{records: []outbox.Record{record(1, "e1", "a")}, failFetches: 2}
	p := &fakePublisher{}
	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()

	_ = newRelay(o, p).Run(ctx)

	if o.failFetches != 0 || len(p.sent) != 1 {
		t.Fatalf("2回失敗したあと送るはずが 残り失敗=%d sent=%v", o.failFetches, p.sent)
	}
}

func runWithLog(t *testing.T, o *fakeOutbox) string {
	t.Helper()
	var buf bytes.Buffer
	r := newRelay(o, &fakePublisher{})
	r.Log = slog.New(slog.NewTextHandler(&buf, nil))
	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()
	_ = r.Run(ctx)
	return buf.String()
}

func TestRunは送れた件数を記録し何も送らなかった周期は記録しない(t *testing.T) {
	logs := runWithLog(t, &fakeOutbox{records: []outbox.Record{record(1, "e1", "a")}})

	if strings.Count(logs, "送信した") != 1 || !strings.Contains(logs, "count=1") || strings.Contains(logs, "count=0") {
		t.Fatalf("1回だけ count=1 を記録するはずが %q", logs)
	}
}

func TestRunは取り出しの失敗を警告として記録する(t *testing.T) {
	logs := runWithLog(t, &fakeOutbox{failFetches: 1})

	if !strings.Contains(logs, "level=WARN") || !strings.Contains(logs, "次の周期でやり直す") || strings.Contains(logs, "送信した") {
		t.Fatalf("警告だけを記録するはずが %q", logs)
	}
}
