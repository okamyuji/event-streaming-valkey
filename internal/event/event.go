// Package event ストリームに流すイベントの形と、パーティションの決め方をまとめる。
package event

import (
	"encoding/json/v2"
	"errors"
	"fmt"
	"hash/fnv"
	"time"
)

const (
	// TypeOrderPlaced 注文が確定したことを表すイベント種別。
	TypeOrderPlaced = "OrderPlaced"
	// TypeOrderCancelled 注文が取り消されたことを表すイベント種別。確定より後に処理しないと結果が崩れる。
	TypeOrderCancelled = "OrderCancelled"
)

// ErrMalformed 何度処理し直しても成功しない、形の壊れたイベントを表す。
var ErrMalformed = errors.New("malformed event")

// Envelope ストリームに載せるイベント1件。中身の型に依存しない共通部分を持つ。
type Envelope struct {
	// ID 読み手が重複を見分けるための一意な値。
	ID   string
	Type string
	// Key 同じ値のイベントは同じパーティションに入り、順序が保たれる。
	Key        string
	OccurredAt time.Time
	Payload    []byte
}

// Fields Valkey Streams のエントリにする field と value の組を返す。
func (e Envelope) Fields() map[string]string {
	return map[string]string{
		"event_id":    e.ID,
		"type":        e.Type,
		"key":         e.Key,
		"occurred_at": e.OccurredAt.UTC().Format(time.RFC3339Nano),
		"payload":     string(e.Payload),
	}
}

// FromFields Valkey Streams のエントリから Envelope を組み立てる。
func FromFields(f map[string]string) (Envelope, error) {
	for _, k := range []string{"event_id", "type", "key", "occurred_at", "payload"} {
		if f[k] == "" {
			return Envelope{}, fmt.Errorf("%w: %s がない", ErrMalformed, k)
		}
	}
	at, err := time.Parse(time.RFC3339Nano, f["occurred_at"])
	if err != nil {
		return Envelope{}, fmt.Errorf("%w: occurred_at: %v", ErrMalformed, err)
	}
	return Envelope{ID: f["event_id"], Type: f["type"], Key: f["key"], OccurredAt: at, Payload: []byte(f["payload"])}, nil
}

// Order 注文確定イベントと注文取消イベントの中身。
type Order struct {
	OrderID   string `json:"order_id"`
	ProductID string `json:"product_id"`
	Quantity  int    `json:"quantity"`
	Amount    int64  `json:"amount"`
}

// Validate 読み手が処理できない注文を ErrMalformed として返す。
func (o Order) Validate() error {
	if o.OrderID == "" {
		return fmt.Errorf("%w: order_id が空", ErrMalformed)
	}
	if o.ProductID == "" {
		return fmt.Errorf("%w: product_id が空", ErrMalformed)
	}
	if o.Quantity <= 0 {
		return fmt.Errorf("%w: quantity は1以上", ErrMalformed)
	}
	if o.Amount < 0 {
		return fmt.Errorf("%w: amount は0以上", ErrMalformed)
	}
	return nil
}

// NewOrderPlaced 注文IDをパーティションキーにした注文確定イベントを作る。
func NewOrderPlaced(id string, at time.Time, o Order) (Envelope, error) {
	return newOrderEvent(TypeOrderPlaced, id, at, o)
}

// NewOrderCancelled 確定イベントと同じ注文IDをキーにした注文取消イベントを作る。同じパーティションに入り、確定より後ろに並ぶ。
func NewOrderCancelled(id string, at time.Time, o Order) (Envelope, error) {
	return newOrderEvent(TypeOrderCancelled, id, at, o)
}

func newOrderEvent(typ, id string, at time.Time, o Order) (Envelope, error) {
	if err := o.Validate(); err != nil {
		return Envelope{}, err
	}
	payload, err := json.Marshal(o)
	if err != nil {
		return Envelope{}, err
	}
	return Envelope{ID: id, Type: typ, Key: o.OrderID, OccurredAt: at, Payload: payload}, nil
}

// DecodeOrder ペイロードを読み、処理できる注文かどうかも確かめる。
func DecodeOrder(payload []byte) (Order, error) {
	var o Order
	if err := json.Unmarshal(payload, &o); err != nil {
		return Order{}, fmt.Errorf("%w: %v", ErrMalformed, err)
	}
	return o, o.Validate()
}

// Partition キーから 0 以上 n 未満のパーティション番号を決める。
func Partition(key string, n int) int {
	h := fnv.New32a()
	_, _ = h.Write([]byte(key))
	return int(h.Sum32() % uint32(n)) // #nosec G115 -- n は設定値で正の小さな数
}

// StreamKey トピック名とパーティション番号から Valkey のキーを作る。
func StreamKey(topic string, partition int) string {
	return fmt.Sprintf("%s.p%d", topic, partition)
}
