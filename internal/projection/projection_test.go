package projection_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/okamyuji/event-streaming-valkey/internal/consumer"
	"github.com/okamyuji/event-streaming-valkey/internal/event"
	"github.com/okamyuji/event-streaming-valkey/internal/projection"
)

type call struct {
	method  string
	name    string
	eventID string
	order   event.Order
	orders  int
	amount  int64
}

type fakeStore struct {
	calls []call
	err   error
}

func (f *fakeStore) Reserve(_ context.Context, group string, e event.Envelope, o event.Order) error {
	f.calls = append(f.calls, call{method: "Reserve", name: group, eventID: e.ID, order: o})
	return f.err
}

func (f *fakeStore) Release(_ context.Context, group string, e event.Envelope, o event.Order) error {
	f.calls = append(f.calls, call{method: "Release", name: group, eventID: e.ID, order: o})
	return f.err
}

func (f *fakeStore) AddSale(_ context.Context, projection string, e event.Envelope, productID string, orders int, amount int64) error {
	f.calls = append(f.calls, call{method: "AddSale", name: projection, eventID: e.ID, order: event.Order{ProductID: productID}, orders: orders, amount: amount})
	return f.err
}

var order = event.Order{OrderID: "order-1", ProductID: "apple", Quantity: 2, Amount: 300}

func envelope(t *testing.T, newEvent func(string, time.Time, event.Order) (event.Envelope, error)) event.Envelope {
	t.Helper()
	e, err := newEvent("e1", time.Now(), order)
	if err != nil {
		t.Fatal(err)
	}
	return e
}

func handlers(s *fakeStore) map[string]consumer.Handler {
	return map[string]consumer.Handler{
		"inventory": projection.Inventory{Store: s, Group: "inventory"},
		"sales":     projection.Sales{Store: s, Projection: "sales"},
	}
}

func Test確定と取消をそれぞれの集計操作に変える(t *testing.T) {
	cases := []struct {
		name     string
		handler  string
		newEvent func(string, time.Time, event.Order) (event.Envelope, error)
		want     call
	}{
		{"在庫は確定で引き当てる", "inventory", event.NewOrderPlaced, call{"Reserve", "inventory", "e1", order, 0, 0}},
		{"在庫は取消で解放する", "inventory", event.NewOrderCancelled, call{"Release", "inventory", "e1", order, 0, 0}},
		{"売上は確定で足す", "sales", event.NewOrderPlaced, call{"AddSale", "sales", "e1", event.Order{ProductID: "apple"}, 1, 300}},
		{"売上は取消で引く", "sales", event.NewOrderCancelled, call{"AddSale", "sales", "e1", event.Order{ProductID: "apple"}, -1, -300}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := &fakeStore{}

			err := handlers(s)[c.handler].Handle(t.Context(), envelope(t, c.newEvent))

			if err != nil {
				t.Fatal(err)
			}
			if len(s.calls) != 1 || s.calls[0] != c.want {
				t.Fatalf("呼び出しが違う: %+v", s.calls)
			}
		})
	}
}

func Test知らない種類のイベントは何もせず成功にする(t *testing.T) {
	for name := range handlers(nil) {
		t.Run(name, func(t *testing.T) {
			s := &fakeStore{}
			e := envelope(t, event.NewOrderPlaced)
			e.Type = "OrderShipped"

			if err := handlers(s)[name].Handle(t.Context(), e); err != nil || len(s.calls) != 0 {
				t.Fatalf("無視するはずが err=%v calls=%v", err, s.calls)
			}
		})
	}
}

func Test壊れたペイロードはErrMalformedを返す(t *testing.T) {
	for name := range handlers(nil) {
		for _, typ := range []string{event.TypeOrderPlaced, event.TypeOrderCancelled} {
			t.Run(name+"/"+typ, func(t *testing.T) {
				s := &fakeStore{}
				e := envelope(t, event.NewOrderPlaced)
				e.Type = typ
				e.Payload = []byte(`{"order_id":"order-1"}`)

				if err := handlers(s)[name].Handle(t.Context(), e); !errors.Is(err, event.ErrMalformed) || len(s.calls) != 0 {
					t.Fatalf("ErrMalformed のはずが err=%v calls=%v", err, s.calls)
				}
			})
		}
	}
}

func Test保存の失敗をそのまま返す(t *testing.T) {
	want := errors.New("DBにつながらない")
	for name := range handlers(nil) {
		for _, newEvent := range []func(string, time.Time, event.Order) (event.Envelope, error){event.NewOrderPlaced, event.NewOrderCancelled} {
			s := &fakeStore{err: want}
			e := envelope(t, newEvent)
			t.Run(name+"/"+e.Type, func(t *testing.T) {
				if err := handlers(s)[name].Handle(t.Context(), e); !errors.Is(err, want) {
					t.Fatalf("保存の失敗を返すはずが %v", err)
				}
			})
		}
	}
}
