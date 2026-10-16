// Package projection イベントを読み手ごとの集計に変える。読み手は互いを知らず、同じイベントから別々の結果を作る。
package projection

import (
	"context"

	"github.com/okamyuji/event-streaming-valkey/internal/event"
)

// InventoryStore 注文ごとの引当を保存する。本番では mysqlstore.Store が満たす。
type InventoryStore interface {
	Reserve(ctx context.Context, group string, e event.Envelope, o event.Order) error
	Release(ctx context.Context, group string, e event.Envelope, o event.Order) error
}

// Inventory 確定で在庫を引き当て、取消で解放する読み手。取消を確定より先に処理すると引当が残る。
type Inventory struct {
	Store InventoryStore
	Group string
}

// Handle 注文のイベントを引当か解放に変え、それ以外の種類は読み飛ばす。
func (h Inventory) Handle(ctx context.Context, e event.Envelope) error {
	o, ok, err := order(e)
	if !ok || err != nil {
		return err
	}
	if e.Type == event.TypeOrderCancelled {
		return h.Store.Release(ctx, h.Group, e, o)
	}
	return h.Store.Reserve(ctx, h.Group, e, o)
}

// SalesStore 売上集計を保存する。本番では mysqlstore.Store が満たす。
type SalesStore interface {
	AddSale(ctx context.Context, projection string, e event.Envelope, productID string, orders int, amount int64) error
}

// Sales 確定で売上に足し、取消で差し引く読み手。
type Sales struct {
	Store SalesStore
	// Projection 集計の置き場所の名前。作り直すときは別の名前にして古い集計と並べる。
	Projection string
}

// Handle 注文のイベントを売上の増減に変え、それ以外の種類は読み飛ばす。
func (h Sales) Handle(ctx context.Context, e event.Envelope) error {
	o, ok, err := order(e)
	if !ok || err != nil {
		return err
	}
	sign := 1
	if e.Type == event.TypeOrderCancelled {
		sign = -1
	}
	return h.Store.AddSale(ctx, h.Projection, e, o.ProductID, sign, int64(sign)*o.Amount)
}

// order 自分に関係のない種類は ok=false で返す。新しい種類が増えても既存の読み手は止まらない。
func order(e event.Envelope) (event.Order, bool, error) {
	if e.Type != event.TypeOrderPlaced && e.Type != event.TypeOrderCancelled {
		return event.Order{}, false, nil
	}
	o, err := event.DecodeOrder(e.Payload)
	return o, true, err
}
