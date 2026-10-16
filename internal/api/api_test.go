package api_test

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/okamyuji/event-streaming-valkey/internal/api"
	"github.com/okamyuji/event-streaming-valkey/internal/event"
	"github.com/okamyuji/event-streaming-valkey/internal/mysqlstore"
)

type fakeStore struct {
	orders     []event.Order
	envelopes  []event.Envelope
	projection string
	cancelled  []string
	err        error
}

func (f *fakeStore) CancelOrder(_ context.Context, orderID, eventID string, at time.Time) (event.Envelope, error) {
	f.cancelled = append(f.cancelled, orderID)
	return event.Envelope{ID: eventID, Key: orderID, OccurredAt: at}, f.err
}

func (f *fakeStore) PlaceOrder(_ context.Context, o event.Order, e event.Envelope) error {
	f.orders = append(f.orders, o)
	f.envelopes = append(f.envelopes, e)
	return f.err
}

func (f *fakeStore) Stock(context.Context) (map[string]int, error) {
	return map[string]int{"apple": 5}, f.err
}

func (f *fakeStore) Sales(_ context.Context, projection string) (map[string]mysqlstore.Sale, error) {
	f.projection = projection
	return map[string]mysqlstore.Sale{"apple": {Orders: 2, Amount: 350}}, f.err
}

var fixedNow = time.Date(2026, 10, 16, 9, 0, 0, 0, time.UTC)

func do(t *testing.T, s *fakeStore, method, path, body string) *httptest.ResponseRecorder {
	t.Helper()
	ids := []string{"order-1", "event-1"}
	h := api.Handler{Store: s, Now: func() time.Time { return fixedNow }, NewID: func() string {
		id := ids[0]
		ids = ids[1:]
		return id
	}}
	rec := httptest.NewRecorder()
	h.Routes().ServeHTTP(rec, httptest.NewRequestWithContext(t.Context(), method, path, strings.NewReader(body)))
	return rec
}

func TestPOSTordersは注文と注文確定イベントを保存して201を返す(t *testing.T) {
	s := &fakeStore{}

	rec := do(t, s, http.MethodPost, "/orders", `{"product_id":"apple","quantity":2,"amount":300}`)

	if rec.Code != http.StatusCreated {
		t.Fatalf("201のはずが %d %s", rec.Code, rec.Body)
	}
	if got := strings.TrimSpace(rec.Body.String()); got != `{"order_id":"order-1","event_id":"event-1"}` {
		t.Fatalf("応答が違う: %s", got)
	}
	want := event.Order{OrderID: "order-1", ProductID: "apple", Quantity: 2, Amount: 300}
	e := s.envelopes[0]
	if s.orders[0] != want || e.ID != "event-1" || e.Key != "order-1" || !e.OccurredAt.Equal(fixedNow) {
		t.Fatalf("保存内容が違う: %+v %+v", s.orders[0], e)
	}
}

func TestPOSTordersは不正な入力に400を返し保存しない(t *testing.T) {
	cases := map[string]string{
		"JSONが壊れている": `{"product_id":`,
		"数量が0":       `{"product_id":"apple","quantity":0,"amount":300}`,
		"商品IDがない":    `{"quantity":1,"amount":300}`,
		"知らない項目がある":  `{"product_id":"apple","quantity":1,"amount":300,"x":1}`,
		"型が違う":       `{"product_id":"apple","quantity":"1","amount":300}`,
		"上限を超える大きさ":  `{"product_id":"` + strings.Repeat("a", 2<<10) + `","quantity":1,"amount":1}`,
		"末尾に余計な値がある": `{"product_id":"apple","quantity":1,"amount":300} {}`,
	}
	for name, body := range cases {
		t.Run(name, func(t *testing.T) {
			s := &fakeStore{}

			rec := do(t, s, http.MethodPost, "/orders", body)

			if rec.Code != http.StatusBadRequest || len(s.orders) != 0 {
				t.Fatalf("400で保存しないはずが %d %d件", rec.Code, len(s.orders))
			}
		})
	}
}

func TestPOSTordersは保存に失敗したら中身を漏らさず500を返す(t *testing.T) {
	s := &fakeStore{err: errors.New("Error 1062: Duplicate entry secret")}

	rec := do(t, s, http.MethodPost, "/orders", `{"product_id":"apple","quantity":1,"amount":100}`)

	if rec.Code != http.StatusInternalServerError || strings.Contains(rec.Body.String(), "secret") {
		t.Fatalf("500で中身を出さないはずが %d %s", rec.Code, rec.Body)
	}
}

func TestGETstockは商品ごとの引当数を返す(t *testing.T) {
	rec := do(t, &fakeStore{}, http.MethodGet, "/stock", "")

	if rec.Code != http.StatusOK || strings.TrimSpace(rec.Body.String()) != `{"apple":5}` {
		t.Fatalf("応答が違う: %d %s", rec.Code, rec.Body)
	}
}

func TestGETsalesは指定した集計先を返し省略時はsalesを使う(t *testing.T) {
	cases := map[string]string{"/sales": "sales", "/sales?projection=sales-replay": "sales-replay"}
	for path, want := range cases {
		t.Run(path, func(t *testing.T) {
			s := &fakeStore{}

			rec := do(t, s, http.MethodGet, path, "")

			if rec.Code != http.StatusOK || s.projection != want {
				t.Fatalf("%s を読むはずが %q (%d)", want, s.projection, rec.Code)
			}
			if got := strings.TrimSpace(rec.Body.String()); got != `{"apple":{"orders":2,"amount":350}}` {
				t.Fatalf("応答が違う: %s", got)
			}
		})
	}
}

func Test読み出しの失敗には500を返す(t *testing.T) {
	for _, path := range []string{"/stock", "/sales"} {
		t.Run(path, func(t *testing.T) {
			rec := do(t, &fakeStore{err: errors.New("DBにつながらない")}, http.MethodGet, path, "")

			if rec.Code != http.StatusInternalServerError {
				t.Fatalf("500のはずが %d", rec.Code)
			}
		})
	}
}

func TestGEThealthzは200を返す(t *testing.T) {
	if rec := do(t, &fakeStore{}, http.MethodGet, "/healthz", ""); rec.Code != http.StatusOK {
		t.Fatalf("200のはずが %d", rec.Code)
	}
}

func Test既定のIDと時刻はUUIDv7と現在時刻を使う(t *testing.T) {
	s := &fakeStore{}
	before := time.Now()
	rec := httptest.NewRecorder()

	api.Handler{Store: s}.Routes().ServeHTTP(rec, httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/orders",
		strings.NewReader(`{"product_id":"apple","quantity":1,"amount":100}`)))

	if rec.Code != http.StatusCreated {
		t.Fatalf("201のはずが %d", rec.Code)
	}
	e := s.envelopes[0]
	if len(e.ID) != 36 || e.ID[14] != '7' || e.ID == e.Key || e.OccurredAt.Before(before) {
		t.Fatalf("UUIDv7 と現在時刻のはずが %+v", e)
	}
}

func TestPOSTcancelは注文を取り消して取消イベントのIDを返す(t *testing.T) {
	s := &fakeStore{}

	rec := do(t, s, http.MethodPost, "/orders/order-9/cancel", "")

	if rec.Code != http.StatusOK {
		t.Fatalf("200のはずが %d %s", rec.Code, rec.Body)
	}
	if got := strings.TrimSpace(rec.Body.String()); got != `{"order_id":"order-9","event_id":"order-1"}` {
		t.Fatalf("応答が違う: %s", got)
	}
	if len(s.cancelled) != 1 || s.cancelled[0] != "order-9" {
		t.Fatalf("order-9 を取り消すはずが %v", s.cancelled)
	}
}

func TestPOSTcancelは失敗の種類に応じた状態コードを返す(t *testing.T) {
	cases := map[string]struct {
		err  error
		want int
	}{
		"注文が無い":   {mysqlstore.ErrOrderNotFound, http.StatusNotFound},
		"取消済み":    {mysqlstore.ErrAlreadyCancelled, http.StatusConflict},
		"それ以外の失敗": {errors.New("DBにつながらない"), http.StatusInternalServerError},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			rec := do(t, &fakeStore{err: c.err}, http.MethodPost, "/orders/order-9/cancel", "")

			if rec.Code != c.want {
				t.Fatalf("%d のはずが %d", c.want, rec.Code)
			}
		})
	}
}

// brokenWriter 応答の本文を書けない接続を表す。
type brokenWriter struct{ header http.Header }

func (b *brokenWriter) Header() http.Header       { return b.header }
func (b *brokenWriter) Write([]byte) (int, error) { return 0, errors.New("接続が切れた") }
func (b *brokenWriter) WriteHeader(int)           {}

func TestGETstockは応答を書けなかったことをログに残す(t *testing.T) {
	var buf bytes.Buffer
	h := api.Handler{Store: &fakeStore{}, Log: slog.New(slog.NewTextHandler(&buf, nil))}

	h.Routes().ServeHTTP(&brokenWriter{header: http.Header{}}, httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/stock", nil))

	if !strings.Contains(buf.String(), "応答の書き込みに失敗") {
		t.Fatalf("ログに残すはずが %q", buf.String())
	}
}

func Test正常に応答できたときはエラーをログに出さない(t *testing.T) {
	var buf bytes.Buffer
	h := api.Handler{Store: &fakeStore{}, Log: slog.New(slog.NewTextHandler(&buf, nil))}
	rec := httptest.NewRecorder()

	h.Routes().ServeHTTP(rec, httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/stock", nil))

	if rec.Code != http.StatusOK || buf.Len() != 0 {
		t.Fatalf("ログは空のはずが %d %q", rec.Code, buf.String())
	}
}
