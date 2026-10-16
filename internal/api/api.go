// Package api 注文を受け付ける書き込み口と、読み手が作った集計を見せる読み出し口。
package api

import (
	"context"
	"encoding/json/v2"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"time"
	"uuid"

	"github.com/okamyuji/event-streaming-valkey/internal/event"
	"github.com/okamyuji/event-streaming-valkey/internal/mysqlstore"
)

const maxBody = 1 << 10

// Store API が使う保存先。本番では mysqlstore.Store が満たす。
type Store interface {
	PlaceOrder(ctx context.Context, o event.Order, e event.Envelope) error
	CancelOrder(ctx context.Context, orderID, eventID string, at time.Time) (event.Envelope, error)
	Stock(ctx context.Context) (map[string]int, error)
	Sales(ctx context.Context, projection string) (map[string]mysqlstore.Sale, error)
}

// Handler HTTP の入口。Now・NewID・Log が空なら現在時刻・UUIDv7・slog の既定のロガーを使う。
type Handler struct {
	Store Store
	Now   func() time.Time
	NewID func() string
	Log   *slog.Logger
}

type orderRequest struct {
	ProductID string `json:"product_id"`
	Quantity  int    `json:"quantity"`
	Amount    int64  `json:"amount"`
}

type orderResponse struct {
	OrderID string `json:"order_id"`
	EventID string `json:"event_id"`
}

// Routes エンドポイントを登録した http.Handler を返す。
func (h Handler) Routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /orders", h.placeOrder)
	mux.HandleFunc("POST /orders/{id}/cancel", h.cancelOrder)
	mux.HandleFunc("GET /stock", h.stock)
	mux.HandleFunc("GET /sales", h.sales)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	// 別のサイトを開いたブラウザから、注文の作成や取消を送らせないようにする。
	return http.NewCrossOriginProtection().Handler(mux)
}

func (h Handler) placeOrder(w http.ResponseWriter, r *http.Request) {
	var req orderRequest
	if err := json.UnmarshalRead(http.MaxBytesReader(w, r.Body, maxBody), &req, json.RejectUnknownMembers(true)); err != nil {
		http.Error(w, "リクエストの形が正しくありません", http.StatusBadRequest)
		return
	}
	o := event.Order{OrderID: h.newID(), ProductID: req.ProductID, Quantity: req.Quantity, Amount: req.Amount}
	e, err := event.NewOrderPlaced(h.newID(), h.now(), o)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if err := h.Store.PlaceOrder(r.Context(), o, e); err != nil {
		h.fail(w, "注文の保存に失敗", err)
		return
	}
	h.writeJSON(w, http.StatusCreated, orderResponse{OrderID: o.OrderID, EventID: e.ID})
}

func (h Handler) cancelOrder(w http.ResponseWriter, r *http.Request) {
	e, err := h.Store.CancelOrder(r.Context(), r.PathValue("id"), h.newID(), h.now())
	if errors.Is(err, mysqlstore.ErrOrderNotFound) {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}
	if errors.Is(err, mysqlstore.ErrAlreadyCancelled) {
		http.Error(w, err.Error(), http.StatusConflict)
		return
	}
	if err != nil {
		h.fail(w, "取消の保存に失敗", err)
		return
	}
	h.writeJSON(w, http.StatusOK, orderResponse{OrderID: e.Key, EventID: e.ID})
}

func (h Handler) stock(w http.ResponseWriter, r *http.Request) {
	v, err := h.Store.Stock(r.Context())
	if err != nil {
		h.fail(w, "在庫の読み出しに失敗", err)
		return
	}
	h.writeJSON(w, http.StatusOK, v)
}

func (h Handler) sales(w http.ResponseWriter, r *http.Request) {
	projection := r.URL.Query().Get("projection")
	if projection == "" {
		projection = "sales"
	}
	v, err := h.Store.Sales(r.Context(), projection)
	if err != nil {
		h.fail(w, "売上の読み出しに失敗", err)
		return
	}
	h.writeJSON(w, http.StatusOK, v)
}

// LoopbackOnly Host がループバックを指す要求だけを通す。
// 別のサイトが自分のドメイン名の向き先を 127.0.0.1 に付け替えると、ブラウザは同じオリジンとして要求を送ってくるため、Host で見分ける。
func LoopbackOnly(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		host, _, err := net.SplitHostPort(r.Host)
		if err != nil {
			host = r.Host
		}
		if host != "localhost" && !net.ParseIP(host).IsLoopback() {
			http.Error(w, "このホスト名では受け付けません", http.StatusForbidden)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// fail 原因はログにだけ残し、利用者には中身を見せない。
func (h Handler) fail(w http.ResponseWriter, msg string, err error) {
	h.log().Error(msg, "err", err)
	http.Error(w, "内部エラー", http.StatusInternalServerError)
}

func (h Handler) log() *slog.Logger {
	if h.Log != nil {
		return h.Log
	}
	return slog.Default()
}

func (h Handler) now() time.Time {
	if h.Now != nil {
		return h.Now()
	}
	return time.Now().UTC()
}

func (h Handler) newID() string {
	if h.NewID != nil {
		return h.NewID()
	}
	return uuid.NewV7().String()
}

func (h Handler) writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.MarshalWrite(w, v, json.Deterministic(true)); err != nil {
		h.log().Error("応答の書き込みに失敗", "err", err)
	}
}
