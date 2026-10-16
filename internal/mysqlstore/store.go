// Package mysqlstore 注文・outbox・読み手ごとの集計を MySQL に置く。
package mysqlstore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/okamyuji/event-streaming-valkey/internal/event"
	"github.com/okamyuji/event-streaming-valkey/internal/outbox"
)

// Store database/sql の接続を包み、各コンポーネントが使う操作を提供する。
type Store struct {
	db *sql.DB
}

// New db を使う Store を作る。
func New(db *sql.DB) *Store {
	return &Store{db: db}
}

var (
	// ErrOrderNotFound 取り消そうとした注文が無い。
	ErrOrderNotFound = errors.New("注文が見つからない")
	// ErrAlreadyCancelled 注文がすでに取り消されている。
	ErrAlreadyCancelled = errors.New("注文はすでに取り消されている")
)

// Sale 商品ごとの売上集計。
type Sale struct {
	Orders int   `json:"orders"`
	Amount int64 `json:"amount"`
}

// PlaceOrder 注文と注文確定イベントを1つのトランザクションで書く。どちらか片方だけが残ることはない。
func (s *Store) PlaceOrder(ctx context.Context, o event.Order, e event.Envelope) error {
	return s.inTx(ctx, func(tx *sql.Tx) error {
		if _, err := tx.ExecContext(ctx,
			"INSERT INTO orders (order_id, product_id, quantity, amount, created_at) VALUES (?, ?, ?, ?, ?)",
			o.OrderID, o.ProductID, o.Quantity, o.Amount, e.OccurredAt); err != nil {
			return fmt.Errorf("注文の書き込み: %w", err)
		}
		return insertOutbox(ctx, tx, e)
	})
}

func insertOutbox(ctx context.Context, tx *sql.Tx, e event.Envelope) error {
	if _, err := tx.ExecContext(ctx,
		"INSERT INTO outbox (event_id, event_type, partition_key, payload, occurred_at) VALUES (?, ?, ?, ?, ?)",
		e.ID, e.Type, e.Key, e.Payload, e.OccurredAt); err != nil {
		return fmt.Errorf("outbox の書き込み: %w", err)
	}
	return nil
}

// CancelOrder 注文に取消の印を付け、元の注文の中身を持つ取消イベントを同じトランザクションで outbox に書く。
func (s *Store) CancelOrder(ctx context.Context, orderID, eventID string, at time.Time) (event.Envelope, error) {
	var e event.Envelope
	err := s.inTx(ctx, func(tx *sql.Tx) error {
		o := event.Order{OrderID: orderID}
		var cancelledAt sql.NullTime
		err := tx.QueryRowContext(ctx,
			"SELECT product_id, quantity, amount, cancelled_at FROM orders WHERE order_id = ? FOR UPDATE", orderID).
			Scan(&o.ProductID, &o.Quantity, &o.Amount, &cancelledAt)
		if errors.Is(err, sql.ErrNoRows) {
			return ErrOrderNotFound
		}
		if err != nil {
			return err
		}
		if cancelledAt.Valid {
			return ErrAlreadyCancelled
		}
		if e, err = event.NewOrderCancelled(eventID, at, o); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, "UPDATE orders SET cancelled_at = ? WHERE order_id = ?", at, orderID); err != nil {
			return err
		}
		return insertOutbox(ctx, tx, e)
	})
	return e, err
}

// Unpublished まだストリームへ送っていないイベントを書き込み順に limit 件まで返す。
func (s *Store) Unpublished(ctx context.Context, limit int) ([]outbox.Record, error) {
	rows, err := s.db.QueryContext(ctx,
		"SELECT seq, event_id, event_type, partition_key, payload, occurred_at FROM outbox WHERE published_at IS NULL ORDER BY seq LIMIT ?",
		limit)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var recs []outbox.Record
	for rows.Next() {
		var r outbox.Record
		e := &r.Envelope
		if err := rows.Scan(&r.Seq, &e.ID, &e.Type, &e.Key, &e.Payload, &e.OccurredAt); err != nil {
			return nil, err
		}
		recs = append(recs, r)
	}
	return recs, rows.Err()
}

// MarkPublished seq の行を送信済みにする。行は消さず、ストリームが切り詰められた後の作り直しにも使える。
func (s *Store) MarkPublished(ctx context.Context, seq int64) error {
	_, err := s.db.ExecContext(ctx, "UPDATE outbox SET published_at = ? WHERE seq = ?", time.Now().UTC(), seq)
	return err
}

// Reserve 注文の数量を引き当てる。group が同じイベントを処理済みなら何もしない。
func (s *Store) Reserve(ctx context.Context, group string, e event.Envelope, o event.Order) error {
	return s.applyOnce(ctx, group, e.ID, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx,
			"INSERT INTO reservations (order_id, product_id, quantity, status) VALUES (?, ?, ?, 'reserved')",
			o.OrderID, o.ProductID, o.Quantity)
		return err
	})
}

// Release 注文の引当を解放する。引当がまだ無ければ何も変わらないので、確定より後に処理する必要がある。
func (s *Store) Release(ctx context.Context, group string, e event.Envelope, o event.Order) error {
	return s.applyOnce(ctx, group, e.ID, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx,
			"UPDATE reservations SET status = 'released' WHERE order_id = ? AND status = 'reserved'", o.OrderID)
		return err
	})
}

// AddSale projection の売上集計に件数と金額を足す。取消では負の値を渡す。同じイベントを処理済みなら何もしない。
func (s *Store) AddSale(ctx context.Context, projection string, e event.Envelope, productID string, orders int, amount int64) error {
	return s.applyOnce(ctx, projection, e.ID, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx,
			`INSERT INTO sales_by_product (projection, product_id, order_count, total_amount) VALUES (?, ?, ?, ?) AS new
			 ON DUPLICATE KEY UPDATE order_count = sales_by_product.order_count + new.order_count, total_amount = sales_by_product.total_amount + new.total_amount`,
			projection, productID, orders, amount)
		return err
	})
}

// Stock 商品ごとの、解放されていない引当数の合計を返す。
func (s *Store) Stock(ctx context.Context) (map[string]int, error) {
	rows, err := s.db.QueryContext(ctx, "SELECT product_id, SUM(quantity) FROM reservations WHERE status = 'reserved' GROUP BY product_id")
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	out := map[string]int{}
	for rows.Next() {
		var id string
		var n int
		if err := rows.Scan(&id, &n); err != nil {
			return nil, err
		}
		out[id] = n
	}
	return out, rows.Err()
}

// Sales projection の商品ごとの売上集計を返す。
func (s *Store) Sales(ctx context.Context, projection string) (map[string]Sale, error) {
	rows, err := s.db.QueryContext(ctx,
		"SELECT product_id, order_count, total_amount FROM sales_by_product WHERE projection = ?", projection)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	out := map[string]Sale{}
	for rows.Next() {
		var id string
		var v Sale
		if err := rows.Scan(&id, &v.Orders, &v.Amount); err != nil {
			return nil, err
		}
		out[id] = v
	}
	return out, rows.Err()
}

// applyOnce 処理済みの記録と集計の更新を同じトランザクションで行い、重複配信を1回分に揃える。
func (s *Store) applyOnce(ctx context.Context, group, eventID string, apply func(*sql.Tx) error) error {
	return s.inTx(ctx, func(tx *sql.Tx) error {
		res, err := tx.ExecContext(ctx,
			// INSERT IGNORE は重複以外のエラー（長すぎる ID の切り詰めなど）も警告に変えてしまうので使わない。
			// 既存の行を同じ値で更新した場合の影響行数は0になり、重複だけを見分けられる。
			"INSERT INTO processed_events (group_name, event_id, processed_at) VALUES (?, ?, ?) ON DUPLICATE KEY UPDATE event_id = event_id",
			group, eventID, time.Now().UTC())
		if err != nil {
			return fmt.Errorf("処理済みの記録: %w", err)
		}
		if n, err := res.RowsAffected(); err != nil || n == 0 {
			return err
		}
		return apply(tx)
	})
}

func (s *Store) inTx(ctx context.Context, fn func(*sql.Tx) error) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	if err := fn(tx); err != nil {
		_ = tx.Rollback()
		return err
	}
	return tx.Commit()
}
