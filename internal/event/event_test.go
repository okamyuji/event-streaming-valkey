package event_test

import (
	"errors"
	"testing"
	"time"

	"github.com/okamyuji/event-streaming-valkey/internal/event"
)

func TestFieldsとFromFieldsで往復しても中身が変わらない(t *testing.T) {
	in := event.Envelope{
		ID:         "e1",
		Type:       event.TypeOrderPlaced,
		Key:        "order-1",
		OccurredAt: time.Date(2026, 10, 16, 9, 0, 0, 123456000, time.UTC),
		Payload:    []byte(`{"order_id":"order-1"}`),
	}

	out, err := event.FromFields(in.Fields())
	if err != nil {
		t.Fatal(err)
	}
	if out.ID != in.ID || out.Type != in.Type || out.Key != in.Key || string(out.Payload) != string(in.Payload) || !out.OccurredAt.Equal(in.OccurredAt) {
		t.Fatalf("往復で変わった: in=%+v out=%+v", in, out)
	}
}

func TestFromFieldsは必須項目が欠けるとErrMalformedを返す(t *testing.T) {
	full := event.Envelope{ID: "e1", Type: "T", Key: "k", OccurredAt: time.Now(), Payload: []byte("{}")}.Fields()
	for _, missing := range []string{"event_id", "type", "key", "occurred_at", "payload"} {
		t.Run(missing, func(t *testing.T) {
			f := map[string]string{}
			for k, v := range full {
				if k != missing {
					f[k] = v
				}
			}
			if _, err := event.FromFields(f); !errors.Is(err, event.ErrMalformed) {
				t.Fatalf("ErrMalformed のはずが %v", err)
			}
		})
	}
}

func TestFromFieldsは時刻が読めないとErrMalformedを返す(t *testing.T) {
	f := event.Envelope{ID: "e1", Type: "T", Key: "k", OccurredAt: time.Now(), Payload: []byte("{}")}.Fields()
	f["occurred_at"] = "昨日"

	if _, err := event.FromFields(f); !errors.Is(err, event.ErrMalformed) {
		t.Fatalf("ErrMalformed のはずが %v", err)
	}
}

func TestNewOrderPlacedはペイロードと注文IDのキーを持つ封筒を作る(t *testing.T) {
	o := event.Order{OrderID: "order-1", ProductID: "apple", Quantity: 2, Amount: 300}
	at := time.Date(2026, 10, 16, 9, 0, 0, 0, time.UTC)

	env, err := event.NewOrderPlaced("e1", at, o)
	if err != nil {
		t.Fatal(err)
	}
	if env.ID != "e1" || env.Type != event.TypeOrderPlaced || env.Key != "order-1" || !env.OccurredAt.Equal(at) {
		t.Fatalf("封筒が違う: %+v", env)
	}
	back, err := event.DecodeOrder(env.Payload)
	if err != nil {
		t.Fatal(err)
	}
	if back != o {
		t.Fatalf("ペイロードが違う: %+v", back)
	}
}

func TestNewOrderPlacedは不正な注文を拒む(t *testing.T) {
	_, err := event.NewOrderPlaced("e1", time.Now(), event.Order{OrderID: "order-1", ProductID: "apple"})
	if !errors.Is(err, event.ErrMalformed) {
		t.Fatalf("ErrMalformed のはずが %v", err)
	}
}

func TestDecodeOrderはJSONが壊れているとErrMalformedを返す(t *testing.T) {
	if _, err := event.DecodeOrder([]byte("{broken")); !errors.Is(err, event.ErrMalformed) {
		t.Fatalf("ErrMalformed のはずが %v", err)
	}
}

func TestOrderPlacedのValidateは境界値を判定する(t *testing.T) {
	valid := event.Order{OrderID: "o", ProductID: "p", Quantity: 1, Amount: 0}
	cases := []struct {
		name string
		edit func(*event.Order)
		ok   bool
	}{
		{"最小の正常値", func(*event.Order) {}, true},
		{"数量0", func(o *event.Order) { o.Quantity = 0 }, false},
		{"数量マイナス", func(o *event.Order) { o.Quantity = -1 }, false},
		{"金額マイナス", func(o *event.Order) { o.Amount = -1 }, false},
		{"注文IDが空", func(o *event.Order) { o.OrderID = "" }, false},
		{"商品IDが空", func(o *event.Order) { o.ProductID = "" }, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			o := valid
			c.edit(&o)
			err := o.Validate()
			if c.ok && err != nil {
				t.Fatalf("正常のはずが %v", err)
			}
			if !c.ok && !errors.Is(err, event.ErrMalformed) {
				t.Fatalf("ErrMalformed のはずが %v", err)
			}
		})
	}
}

func TestPartitionは同じキーに同じ番号を返し範囲に収まる(t *testing.T) {
	seen := map[int]bool{}
	for _, key := range []string{"a", "b", "c", "d", "e", "f", "g", "h"} {
		p := event.Partition(key, 3)
		if p != event.Partition(key, 3) {
			t.Fatalf("同じキー %q で番号が変わった", key)
		}
		if p < 0 || p >= 3 {
			t.Fatalf("範囲外 %d", p)
		}
		seen[p] = true
	}
	if len(seen) < 2 {
		t.Fatalf("8種類のキーが1つのパーティションに偏った: %v", seen)
	}
}

func TestPartitionは分割数1なら常に0を返す(t *testing.T) {
	if p := event.Partition("anything", 1); p != 0 {
		t.Fatalf("0のはずが %d", p)
	}
}

func TestStreamKeyはトピックとパーティション番号からキーを作る(t *testing.T) {
	if got := event.StreamKey("orders", 2); got != "orders.p2" {
		t.Fatalf("orders.p2 のはずが %s", got)
	}
}

func TestNewOrderCancelledは同じ注文IDのキーを持つ取消イベントを作る(t *testing.T) {
	o := event.Order{OrderID: "order-1", ProductID: "apple", Quantity: 2, Amount: 300}
	at := time.Date(2026, 10, 16, 9, 0, 0, 0, time.UTC)

	env, err := event.NewOrderCancelled("e2", at, o)
	if err != nil {
		t.Fatal(err)
	}
	if env.ID != "e2" || env.Type != event.TypeOrderCancelled || env.Key != "order-1" || !env.OccurredAt.Equal(at) {
		t.Fatalf("封筒が違う: %+v", env)
	}
	back, err := event.DecodeOrder(env.Payload)
	if err != nil || back != o {
		t.Fatalf("ペイロードが違う: %+v %v", back, err)
	}
}

func TestNewOrderCancelledは不正な注文を拒む(t *testing.T) {
	if _, err := event.NewOrderCancelled("e2", time.Now(), event.Order{OrderID: "order-1"}); !errors.Is(err, event.ErrMalformed) {
		t.Fatalf("ErrMalformed のはずが %v", err)
	}
}
