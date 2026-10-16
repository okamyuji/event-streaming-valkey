package mysqlstore_test

import (
	"context"
	"database/sql"
	"errors"
	"flag"
	"log"
	"os"
	"strings"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/okamyuji/event-streaming-valkey/internal/event"
	"github.com/okamyuji/event-streaming-valkey/internal/mysqlstore"
	tcmysql "github.com/testcontainers/testcontainers-go/modules/mysql"
)

var dsn string

// TestMain パッケージ内のテストで MySQL コンテナを1つ共有する。ミューテーションテストの実行時間を抑えるため。
func TestMain(m *testing.M) {
	flag.Parse()
	if testing.Short() {
		os.Exit(m.Run())
	}
	ctx := context.Background()
	c, err := tcmysql.Run(ctx, "mysql:8.4", tcmysql.WithDatabase("events"), tcmysql.WithScripts("../../db/init/001_schema.sql"))
	if err != nil {
		log.Fatalf("MySQLコンテナの起動に失敗: %v", err)
	}
	if dsn, err = c.ConnectionString(ctx, "parseTime=true", "loc=UTC"); err != nil {
		log.Fatal(err)
	}
	code := m.Run()
	_ = c.Terminate(ctx)
	os.Exit(code)
}

// newStore テストごとに表を空にし、閉じても他のテストに響かない専用の接続を返す。
func newStore(t *testing.T) (*mysqlstore.Store, *sql.DB) {
	t.Helper()
	if testing.Short() {
		t.Skip("-short ではコンテナを使うテストを飛ばす")
	}
	db, err := sql.Open("mysql", dsn)
	must(t, err)
	t.Cleanup(func() { _ = db.Close() })
	for _, table := range []string{"orders", "outbox", "processed_events", "reservations", "sales_by_product"} {
		_, err := db.ExecContext(t.Context(), "TRUNCATE TABLE "+table)
		must(t, err)
	}
	return mysqlstore.New(db), db
}

func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

func placed(t *testing.T, eventID, orderID, product string, qty int, amount int64) (event.Order, event.Envelope) {
	t.Helper()
	o := event.Order{OrderID: orderID, ProductID: product, Quantity: qty, Amount: amount}
	env, err := event.NewOrderPlaced(eventID, time.Date(2026, 10, 16, 9, 0, 0, 123456000, time.UTC), o)
	must(t, err)
	return o, env
}

func count(t *testing.T, db *sql.DB, q string) int {
	t.Helper()
	var n int
	must(t, db.QueryRowContext(t.Context(), q).Scan(&n))
	return n
}

func TestPlaceOrderは注文とoutboxを同時に書き未送信として取り出せる(t *testing.T) {
	s, db := newStore(t)
	ctx := t.Context()
	o, env := placed(t, "e1", "order-1", "apple", 2, 300)

	must(t, s.PlaceOrder(ctx, o, env))

	if n := count(t, db, "SELECT COUNT(*) FROM orders"); n != 1 {
		t.Fatalf("注文は1件のはずが %d", n)
	}
	recs, err := s.Unpublished(ctx, 10)
	must(t, err)
	if len(recs) != 1 {
		t.Fatalf("未送信は1件のはずが %d", len(recs))
	}
	got := recs[0].Envelope
	back, err := event.DecodeOrder(got.Payload)
	must(t, err)
	if got.ID != "e1" || got.Type != event.TypeOrderPlaced || got.Key != "order-1" || !got.OccurredAt.Equal(env.OccurredAt) || back != o {
		t.Fatalf("取り出した内容が違う: %+v %+v", got, back)
	}
}

func TestPlaceOrderは注文が書けなければoutboxも書かない(t *testing.T) {
	s, db := newStore(t)
	ctx := t.Context()
	o, env := placed(t, "e1", "order-1", "apple", 1, 100)
	must(t, s.PlaceOrder(ctx, o, env))
	_, dup := placed(t, "e2", "order-1", "apple", 1, 100)

	if err := s.PlaceOrder(ctx, o, dup); err == nil {
		t.Fatal("同じ注文IDの2回目は失敗するはず")
	}
	if n := count(t, db, "SELECT COUNT(*) FROM outbox"); n != 1 {
		t.Fatalf("outbox は1件のままのはずが %d", n)
	}
}

func TestUnpublishedは古い順に件数を絞って返し送信済みを除く(t *testing.T) {
	s, _ := newStore(t)
	ctx := t.Context()
	for _, id := range []string{"1", "2", "3"} {
		o, env := placed(t, "e"+id, "order-"+id, "apple", 1, 100)
		must(t, s.PlaceOrder(ctx, o, env))
	}

	first, err := s.Unpublished(ctx, 2)
	must(t, err)
	if len(first) != 2 || first[0].Envelope.ID != "e1" || first[1].Envelope.ID != "e2" {
		t.Fatalf("古い順に2件のはずが %+v", first)
	}
	must(t, s.MarkPublished(ctx, first[0].Seq))
	rest, err := s.Unpublished(ctx, 10)
	must(t, err)
	if len(rest) != 2 || rest[0].Envelope.ID != "e2" {
		t.Fatalf("送信済みを除くはずが %+v", rest)
	}
}

func TestCancelOrderは注文に取消の印を付け取消イベントをoutboxに書く(t *testing.T) {
	s, _ := newStore(t)
	ctx := t.Context()
	o, placedEnv := placed(t, "e1", "order-1", "apple", 2, 300)
	must(t, s.PlaceOrder(ctx, o, placedEnv))
	at := time.Date(2026, 10, 16, 10, 0, 0, 0, time.UTC)

	e, err := s.CancelOrder(ctx, "order-1", "e2", at)

	must(t, err)
	if e.ID != "e2" || e.Type != event.TypeOrderCancelled || e.Key != "order-1" || !e.OccurredAt.Equal(at) {
		t.Fatalf("取消イベントが違う: %+v", e)
	}
	back, err := event.DecodeOrder(e.Payload)
	must(t, err)
	if back != o {
		t.Fatalf("取消イベントの中身は元の注文のはずが %+v", back)
	}
	recs, err := s.Unpublished(ctx, 10)
	must(t, err)
	if len(recs) != 2 || recs[1].Envelope.ID != "e2" {
		t.Fatalf("確定→取消の順に並ぶはずが %+v", recs)
	}
}

func TestCancelOrderは無い注文と取消済みの注文を拒みoutboxに書かない(t *testing.T) {
	s, db := newStore(t)
	ctx := t.Context()
	o, e := placed(t, "e1", "order-1", "apple", 1, 100)
	must(t, s.PlaceOrder(ctx, o, e))
	_, err := s.CancelOrder(ctx, "order-1", "e2", time.Now())
	must(t, err)

	if _, err := s.CancelOrder(ctx, "order-1", "e3", time.Now()); !errors.Is(err, mysqlstore.ErrAlreadyCancelled) {
		t.Fatalf("ErrAlreadyCancelled のはずが %v", err)
	}
	if _, err := s.CancelOrder(ctx, "order-x", "e4", time.Now()); !errors.Is(err, mysqlstore.ErrOrderNotFound) {
		t.Fatalf("ErrOrderNotFound のはずが %v", err)
	}
	if n := count(t, db, "SELECT COUNT(*) FROM outbox"); n != 2 {
		t.Fatalf("outbox は確定と1回目の取消の2件のはずが %d", n)
	}
}

func TestReserveは同じイベントを2回受けても1回分だけ引き当てる(t *testing.T) {
	s, _ := newStore(t)
	ctx := t.Context()
	o1, e1 := placed(t, "e1", "order-1", "apple", 2, 300)
	o2, e2 := placed(t, "e2", "order-2", "apple", 3, 450)

	must(t, s.Reserve(ctx, "inventory", e1, o1))
	must(t, s.Reserve(ctx, "inventory", e1, o1))
	must(t, s.Reserve(ctx, "inventory", e2, o2))

	stock, err := s.Stock(ctx)
	must(t, err)
	if stock["apple"] != 5 {
		t.Fatalf("2+3=5 のはずが %d", stock["apple"])
	}
}

func TestReleaseは確定の後なら引当を解放する(t *testing.T) {
	s, _ := newStore(t)
	ctx := t.Context()
	o1, e1 := placed(t, "e1", "order-1", "apple", 2, 300)
	o2, e2 := placed(t, "e2", "order-2", "apple", 3, 450)
	must(t, s.Reserve(ctx, "inventory", e1, o1))
	must(t, s.Reserve(ctx, "inventory", e2, o2))
	_, cancel := placed(t, "e3", "order-1", "apple", 2, 300)

	must(t, s.Release(ctx, "inventory", cancel, o1))
	must(t, s.Release(ctx, "inventory", cancel, o1))

	stock, err := s.Stock(ctx)
	must(t, err)
	if stock["apple"] != 3 {
		t.Fatalf("order-1 の2個だけ解放して3のはずが %d", stock["apple"])
	}
}

func TestReleaseが確定より先に届くと解放する行がなく引当が残る(t *testing.T) {
	s, _ := newStore(t)
	ctx := t.Context()
	o, placedEnv := placed(t, "e1", "order-1", "apple", 2, 300)
	_, cancel := placed(t, "e2", "order-1", "apple", 2, 300)

	must(t, s.Release(ctx, "inventory", cancel, o))
	must(t, s.Reserve(ctx, "inventory", placedEnv, o))

	stock, err := s.Stock(ctx)
	must(t, err)
	if stock["apple"] != 2 {
		t.Fatalf("順番が逆だと取り消した注文の2個が残るはずが %d", stock["apple"])
	}
}

func TestAddSaleは集計先ごとに別々に重複を除いて積み上げ取消で差し引く(t *testing.T) {
	s, _ := newStore(t)
	ctx := t.Context()
	_, e1 := placed(t, "e1", "order-1", "apple", 1, 100)
	_, e2 := placed(t, "e2", "order-2", "apple", 1, 250)
	_, e3 := placed(t, "e3", "order-1", "apple", 1, 100)

	must(t, s.AddSale(ctx, "sales", e1, "apple", 1, 100))
	must(t, s.AddSale(ctx, "sales", e1, "apple", 1, 100))
	must(t, s.AddSale(ctx, "sales", e2, "apple", 1, 250))
	must(t, s.AddSale(ctx, "sales", e3, "apple", -1, -100))
	must(t, s.AddSale(ctx, "sales-replay", e1, "apple", 1, 100))

	sales, err := s.Sales(ctx, "sales")
	must(t, err)
	if got := sales["apple"]; got.Orders != 1 || got.Amount != 250 {
		t.Fatalf("sales は1件250円のはずが %+v", got)
	}
	replay, err := s.Sales(ctx, "sales-replay")
	must(t, err)
	if got := replay["apple"]; got.Orders != 1 || got.Amount != 100 {
		t.Fatalf("sales-replay は1件100円のはずが %+v", got)
	}
}

func TestStockとSalesはデータが無ければ空を返す(t *testing.T) {
	s, _ := newStore(t)
	ctx := t.Context()

	stock, err := s.Stock(ctx)
	must(t, err)
	sales, err := s.Sales(ctx, "sales")
	must(t, err)
	if len(stock) != 0 || len(sales) != 0 {
		t.Fatalf("空のはずが stock=%v sales=%v", stock, sales)
	}
}

func Test閉じたDBでは各操作がエラーを返す(t *testing.T) {
	s, db := newStore(t)
	ctx := t.Context()
	o, env := placed(t, "e1", "order-1", "apple", 1, 100)
	must(t, db.Close())

	checks := map[string]error{
		"PlaceOrder":    s.PlaceOrder(ctx, o, env),
		"MarkPublished": s.MarkPublished(ctx, 1),
		"Reserve":       s.Reserve(ctx, "inventory", env, o),
		"Release":       s.Release(ctx, "inventory", env, o),
		"AddSale":       s.AddSale(ctx, "sales", env, "apple", 1, 100),
	}
	_, checks["CancelOrder"] = s.CancelOrder(ctx, "order-1", "e2", time.Now())
	_, checks["Unpublished"] = s.Unpublished(ctx, 1)
	_, checks["Stock"] = s.Stock(ctx)
	_, checks["Sales"] = s.Sales(ctx, "sales")
	for name, err := range checks {
		if err == nil {
			t.Errorf("%s はエラーを返すはず", name)
		}
	}
}

func Test処理済みの記録に入らない長さのイベントIDは黙って切り詰めずエラーを返す(t *testing.T) {
	s, _ := newStore(t)
	ctx := t.Context()
	o, e := placed(t, "e1", "order-1", "apple", 1, 100)
	e.ID = strings.Repeat("x", 37)

	if err := s.Reserve(ctx, "inventory", e, o); err == nil {
		t.Fatal("37文字のイベントIDはエラーになるはず")
	}
	stock, err := s.Stock(ctx)
	must(t, err)
	if len(stock) != 0 {
		t.Fatalf("引当は増えないはずが %v", stock)
	}
}
