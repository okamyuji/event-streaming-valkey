package stream_test

import (
	"context"
	"flag"
	"log"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/okamyuji/event-streaming-valkey/internal/event"
	"github.com/okamyuji/event-streaming-valkey/internal/stream"
	tcvalkey "github.com/testcontainers/testcontainers-go/modules/valkey"
	"github.com/valkey-io/valkey-go"
)

var shared valkey.Client

// TestMain パッケージ内のテストで Valkey コンテナを1つ共有する。ミューテーションテストの実行時間を抑えるため。
func TestMain(m *testing.M) {
	flag.Parse()
	if testing.Short() {
		os.Exit(m.Run())
	}
	ctx := context.Background()
	c, err := tcvalkey.Run(ctx, "valkey/valkey:9.1")
	if err != nil {
		log.Fatalf("Valkeyコンテナの起動に失敗: %v", err)
	}
	addr, err := c.ConnectionString(ctx)
	if err != nil {
		log.Fatal(err)
	}
	opt, err := valkey.ParseURL(addr)
	if err != nil {
		log.Fatal(err)
	}
	if shared, err = valkey.NewClient(opt); err != nil {
		log.Fatal(err)
	}
	code := m.Run()
	shared.Close()
	_ = c.Terminate(ctx)
	os.Exit(code)
}

func newValkey(t *testing.T, maxLen int64) (*stream.Valkey, valkey.Client) {
	t.Helper()
	if testing.Short() {
		t.Skip("-short ではコンテナを使うテストを飛ばす")
	}
	must(t, shared.Do(t.Context(), shared.B().Flushall().Build()).Error())
	return stream.NewValkey(shared, maxLen), shared
}

func sampleEnvelope(id string) event.Envelope {
	return event.Envelope{
		ID:         id,
		Type:       event.TypeOrderPlaced,
		Key:        "order-1",
		OccurredAt: time.Date(2026, 10, 16, 9, 0, 0, 0, time.UTC),
		Payload:    []byte(`{"order_id":"order-1","product_id":"apple","quantity":1,"amount":100}`),
	}
}

func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

// publishAndRead グループを作って1件流し、consumer に1回配信した状態を作る。
func publishAndRead(t *testing.T, v *stream.Valkey, consumer string) stream.Message {
	t.Helper()
	ctx := t.Context()
	must(t, v.EnsureGroup(ctx, "orders.p0", "inventory", "0"))
	_, err := v.Publish(ctx, "orders.p0", sampleEnvelope("e1"))
	must(t, err)
	msgs, err := v.Read(ctx, "orders.p0", "inventory", consumer, ">", 10, 100*time.Millisecond)
	must(t, err)
	if len(msgs) != 1 {
		t.Fatalf("1件読めるはずが %d 件", len(msgs))
	}
	return msgs[0]
}

func TestPublishしたイベントをグループで読める(t *testing.T) {
	v, _ := newValkey(t, 1000)
	ctx := t.Context()

	if err := v.EnsureGroup(ctx, "orders.p0", "inventory", "0"); err != nil {
		t.Fatal(err)
	}
	if _, err := v.Publish(ctx, "orders.p0", sampleEnvelope("e1")); err != nil {
		t.Fatal(err)
	}

	msgs, err := v.Read(ctx, "orders.p0", "inventory", "c1", ">", 10, 100*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	if len(msgs) != 1 {
		t.Fatalf("1件読めるはずが %d 件", len(msgs))
	}
	got, err := event.FromFields(msgs[0].Fields)
	if err != nil {
		t.Fatal(err)
	}
	if got.ID != "e1" || got.Key != "order-1" {
		t.Fatalf("読んだイベントが違う: %+v", got)
	}
}

func TestEnsureGroupは2回呼んでも失敗しない(t *testing.T) {
	v, _ := newValkey(t, 1000)
	ctx := t.Context()

	for range 2 {
		if err := v.EnsureGroup(ctx, "orders.p0", "inventory", "0"); err != nil {
			t.Fatalf("既存グループで失敗した: %v", err)
		}
	}
}

func Test新しいメッセージが無いときReadは空を返す(t *testing.T) {
	v, _ := newValkey(t, 1000)
	ctx := t.Context()
	if err := v.EnsureGroup(ctx, "orders.p0", "inventory", "0"); err != nil {
		t.Fatal(err)
	}

	msgs, err := v.Read(ctx, "orders.p0", "inventory", "c1", ">", 10, 50*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	if len(msgs) != 0 {
		t.Fatalf("空のはずが %d 件", len(msgs))
	}
}

func TestACKしないメッセージはID0で読み直せてACK後は消える(t *testing.T) {
	v, _ := newValkey(t, 1000)
	ctx := t.Context()
	first := publishAndRead(t, v, "c1")

	pending, err := v.Read(ctx, "orders.p0", "inventory", "c1", "0", 10, time.Hour)
	must(t, err)
	if len(pending) != 1 || pending[0].ID != first.ID {
		t.Fatalf("未ACK分が読み直せない: %+v", pending)
	}

	must(t, v.Ack(ctx, "orders.p0", "inventory", first.ID))
	pending, err = v.Read(ctx, "orders.p0", "inventory", "c1", "0", 10, time.Hour)
	must(t, err)
	if len(pending) != 0 {
		t.Fatalf("ACK後も残っている: %+v", pending)
	}
}

func TestClaimは放置時間に届かないメッセージを奪わない(t *testing.T) {
	v, _ := newValkey(t, 1000)
	publishAndRead(t, v, "dead")

	claimed, err := v.Claim(t.Context(), "orders.p0", "inventory", "alive", time.Hour, 10)
	must(t, err)
	if len(claimed) != 0 {
		t.Fatalf("放置時間に届かないのに奪えた: %+v", claimed)
	}
}

func Test放置時間0のClaimは他の持ち主の未ACK分をIDの古い順に全部引き取る(t *testing.T) {
	v, _ := newValkey(t, 1000)
	ctx := t.Context()
	must(t, v.EnsureGroup(ctx, "orders.p0", "inventory", "0"))
	for _, id := range []string{"e1", "e2"} {
		_, err := v.Publish(ctx, "orders.p0", sampleEnvelope(id))
		must(t, err)
	}
	read, err := v.Read(ctx, "orders.p0", "inventory", "dead", ">", 10, 100*time.Millisecond)
	must(t, err)

	claimed, err := v.Claim(ctx, "orders.p0", "inventory", "alive", 0, 10)
	must(t, err)
	if len(claimed) != 2 || claimed[0].ID != read[0].ID || claimed[1].ID != read[1].ID {
		t.Fatalf("2件を古い順に引き取るはずが %+v", claimed)
	}
	mine, err := v.Read(ctx, "orders.p0", "inventory", "alive", "0", 10, time.Hour)
	must(t, err)
	if len(mine) != 2 {
		t.Fatalf("引き取った分が alive の未ACKになるはずが %+v", mine)
	}
}

func TestDeadLetterは元の場所と理由を付けて別ストリームに書く(t *testing.T) {
	v, client := newValkey(t, 1000)
	ctx := t.Context()
	m := stream.Message{ID: "5-0", Fields: map[string]string{"event_id": "e1", "payload": "{broken"}}

	if err := v.DeadLetter(ctx, "orders.dlq", "orders.p0", m, "payload が壊れている"); err != nil {
		t.Fatal(err)
	}

	entries, err := client.Do(ctx, client.B().Xrange().Key("orders.dlq").Start("-").End("+").Build()).AsXRange()
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 {
		t.Fatalf("DLQに1件のはずが %d 件", len(entries))
	}
	f := entries[0].FieldValues
	if f["source_stream"] != "orders.p0" || f["source_id"] != "5-0" || f["reason"] != "payload が壊れている" || f["payload"] != "{broken" {
		t.Fatalf("DLQの中身が違う: %+v", f)
	}
}

func TestPublishはMAXLENで古いイベントを捨てる(t *testing.T) {
	// "~" 付きの削除は内部ノード単位（既定100件）で行うので、件数は上限ちょうどにはならない。
	v, client := newValkey(t, 100)
	ctx := t.Context()

	for i := range 500 {
		_, err := v.Publish(ctx, "orders.p0", sampleEnvelope("e"+strconv.Itoa(i)))
		must(t, err)
	}

	n, err := client.Do(ctx, client.B().Xlen().Key("orders.p0").Build()).AsInt64()
	must(t, err)
	if n < 100 || n > 200 {
		t.Fatalf("MAXLEN ~ 100 なら100〜200件のはずが %d 件", n)
	}
}
