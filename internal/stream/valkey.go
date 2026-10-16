// Package stream Valkey Streams をイベントログとして使うための薄いアダプタ。
package stream

import (
	"context"
	"strconv"
	"time"

	"github.com/okamyuji/event-streaming-valkey/internal/event"
	"github.com/valkey-io/valkey-go"
)

// Message ストリームから読んだエントリ1件。
type Message struct {
	ID     string
	Fields map[string]string
}

// Valkey Valkey Streams への書き込みとコンシューマグループでの読み出しを担う。
type Valkey struct {
	c valkey.Client
	// maxLen ストリームに残すおおよその件数。再生できる過去の範囲はこれで決まる。
	maxLen string
}

// NewValkey maxLen 件を目安に古いエントリを捨てる Valkey を作る。
func NewValkey(c valkey.Client, maxLen int64) *Valkey {
	return &Valkey{c: c, maxLen: strconv.FormatInt(maxLen, 10)}
}

// Publish イベントをストリームの末尾に追記し、Valkey が振ったエントリIDを返す。
func (v *Valkey) Publish(ctx context.Context, stream string, e event.Envelope) (string, error) {
	fv := v.c.B().Xadd().Key(stream).Maxlen().Almost().Threshold(v.maxLen).Id("*").FieldValue()
	for k, val := range e.Fields() {
		fv = fv.FieldValue(k, val)
	}
	return v.c.Do(ctx, fv.Build()).ToString()
}

// EnsureGroup コンシューマグループを start の位置から作る。既にあれば何もしない。
func (v *Valkey) EnsureGroup(ctx context.Context, stream, group, start string) error {
	err := v.c.Do(ctx, v.c.B().XgroupCreate().Key(stream).Group(group).Id(start).Mkstream().Build()).Error()
	if valkey.IsValkeyBusyGroup(err) {
		return nil
	}
	return err
}

// Read id に ">" を渡すと未配信の新着を block の間待って読み、"0" などを渡すと自分宛ての未ACK分を待たずに読む。
func (v *Valkey) Read(ctx context.Context, stream, group, consumer, id string, count int64, block time.Duration) ([]Message, error) {
	cmd := v.c.B().Xreadgroup().Group(group, consumer).Count(count).Block(block.Milliseconds()).Streams().Key(stream).Id(id).Build()
	res, err := v.c.Do(ctx, cmd).AsXRead()
	if valkey.IsValkeyNil(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return toMessages(res[stream]), nil
}

// Claim minIdle より長く ACK されていないエントリを、ID の古い順に consumer へ移す。止まった前の持ち主の分を引き取るのに使う。
func (v *Valkey) Claim(ctx context.Context, stream, group, consumer string, minIdle time.Duration, count int64) ([]Message, error) {
	cmd := v.c.B().Xautoclaim().Key(stream).Group(group).Consumer(consumer).
		MinIdleTime(strconv.FormatInt(minIdle.Milliseconds(), 10)).Start("0-0").Count(count).Build()
	reply, err := v.c.Do(ctx, cmd).ToArray()
	if err != nil {
		return nil, err
	}
	// 応答は [次の開始ID, 奪ったエントリ, 消えていたID] の3要素。
	entries, err := reply[1].AsXRange()
	if err != nil {
		return nil, err
	}
	return toMessages(entries), nil
}

// Ack 処理を終えたエントリを未ACK一覧から外す。
func (v *Valkey) Ack(ctx context.Context, stream, group, id string) error {
	return v.c.Do(ctx, v.c.B().Xack().Key(stream).Group(group).Id(id).Build()).Error()
}

// DeadLetter 処理できないエントリを、元の場所と理由を添えて退避用ストリームへ書く。
func (v *Valkey) DeadLetter(ctx context.Context, dlq, source string, m Message, reason string) error {
	meta := map[string]string{"source_stream": source, "source_id": m.ID, "reason": reason}
	fv := v.c.B().Xadd().Key(dlq).Maxlen().Almost().Threshold(v.maxLen).Id("*").FieldValue()
	for k, val := range m.Fields {
		// 元のエントリに同じ名前があっても、退避の情報を上書きさせない。
		if _, reserved := meta[k]; !reserved {
			fv = fv.FieldValue(k, val)
		}
	}
	for k, val := range meta {
		fv = fv.FieldValue(k, val)
	}
	return v.c.Do(ctx, fv.Build()).Error()
}

func toMessages(entries []valkey.XRangeEntry) []Message {
	msgs := make([]Message, 0, len(entries))
	for _, e := range entries {
		msgs = append(msgs, Message{ID: e.ID, Fields: e.FieldValues})
	}
	return msgs
}
