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

// 上限は1件の値が MySQL の列に収まる範囲から決めている。累計の溢れは集計の更新がエラーにし、読み手が DLQ へ退避する。
const (
	maxEventIDLen   = 36
	maxOrderIDLen   = 36
	maxProductIDLen = 64
	// MaxQuantity 1件の注文で受け付ける数量の上限。
	MaxQuantity = 1_000_000
	// MaxAmount 1件の注文で受け付ける金額の上限。
	MaxAmount = 1_000_000_000_000
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
	if len(f["event_id"]) > maxEventIDLen {
		return Envelope{}, fmt.Errorf("%w: event_id は%dバイト以内", ErrMalformed, maxEventIDLen)
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
	if o.OrderID == "" || len(o.OrderID) > maxOrderIDLen {
		return fmt.Errorf("%w: order_id は1〜%dバイト", ErrMalformed, maxOrderIDLen)
	}
	if o.ProductID == "" || len(o.ProductID) > maxProductIDLen {
		return fmt.Errorf("%w: product_id は1〜%dバイト", ErrMalformed, maxProductIDLen)
	}
	if o.Quantity <= 0 || o.Quantity > MaxQuantity {
		return fmt.Errorf("%w: quantity は1〜%d", ErrMalformed, MaxQuantity)
	}
	if o.Amount < 0 || o.Amount > MaxAmount {
		return fmt.Errorf("%w: amount は0〜%d", ErrMalformed, MaxAmount)
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
