package app_test

import (
	"context"
	"database/sql"
	"errors"
	"flag"
	"log"
	"net"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/okamyuji/event-streaming-valkey/internal/app"
	"github.com/okamyuji/event-streaming-valkey/internal/config"
	"github.com/okamyuji/event-streaming-valkey/internal/event"
	"github.com/okamyuji/event-streaming-valkey/internal/mysqlstore"
	"github.com/okamyuji/event-streaming-valkey/internal/stream"
	tcmysql "github.com/testcontainers/testcontainers-go/modules/mysql"
	tcvalkey "github.com/testcontainers/testcontainers-go/modules/valkey"
	"github.com/valkey-io/valkey-go"
)

var (
	mysqlDSN   string
	valkeyAddr string
)

// TestMain 配線の確認に使う MySQL と Valkey を1つずつ起動して共有する。
func TestMain(m *testing.M) {
	flag.Parse()
	if testing.Short() {
		os.Exit(m.Run())
	}
	ctx := context.Background()
	my, err := tcmysql.Run(ctx, "mysql:8.4", tcmysql.WithDatabase("events"), tcmysql.WithScripts("../../db/init/001_schema.sql"))
	if err != nil {
		log.Fatalf("MySQLコンテナの起動に失敗: %v", err)
	}
	vk, err := tcvalkey.Run(ctx, "valkey/valkey:9.1")
	if err != nil {
		log.Fatalf("Valkeyコンテナの起動に失敗: %v", err)
	}
	if mysqlDSN, err = my.ConnectionString(ctx, "parseTime=true", "loc=UTC"); err != nil {
		log.Fatal(err)
	}
	if valkeyAddr, err = vk.Endpoint(ctx, ""); err != nil {
		log.Fatal(err)
	}
	code := m.Run()
	_ = my.Terminate(ctx)
	_ = vk.Terminate(ctx)
	os.Exit(code)
}

func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

func freeAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	must(t, err)
	addr := l.Addr().String()
	_ = l.Close()
	return addr
}

// fixture 表と Valkey を空にし、実コンテナを向いた設定と操作用の接続を返す。
func fixture(t *testing.T) (config.Config, *mysqlstore.Store, valkey.Client) {
	t.Helper()
	if testing.Short() {
		t.Skip("-short ではコンテナを使うテストを飛ばす")
	}
	db, err := sql.Open("mysql", mysqlDSN)
	must(t, err)
	t.Cleanup(func() { _ = db.Close() })
	for _, table := range []string{"orders", "outbox", "processed_events", "reservations", "sales_by_product"} {
		_, err := db.ExecContext(t.Context(), "TRUNCATE TABLE "+table)
		must(t, err)
	}
	vk, err := valkey.NewClient(valkey.ClientOption{InitAddress: []string{valkeyAddr}})
	must(t, err)
	t.Cleanup(vk.Close)
	must(t, vk.Do(t.Context(), vk.B().Flushall().Build()).Error())
	cfg, err := config.Load(func(k string) string {
		return map[string]string{
			"MYSQL_DSN": mysqlDSN, "VALKEY_ADDR": valkeyAddr, "HTTP_ADDR": freeAddr(t), "PARTITIONS": "2",
		}[k]
	})
	must(t, err)
	return cfg, mysqlstore.New(db), vk
}

// start run を別の goroutine で動かし、止めて戻り値を受け取る関数を返す。
func start(t *testing.T, run app.RunFunc, cfg config.Config) func() error {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- run(ctx, cfg) }()
	return func() error {
		cancel()
		select {
		case err := <-done:
			return err
		case <-time.After(10 * time.Second):
			t.Fatal("停止の合図から10秒たっても止まらない")
			return nil
		}
	}
}

func eventually(t *testing.T, what string, ok func() bool) {
	t.Helper()
	for range 100 {
		if ok() {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("5秒待っても %s にならない", what)
}

func TestMainは設定が不正なら2を返しrunを呼ばない(t *testing.T) {
	called := false

	code := app.Main("test", func(context.Context, config.Config) error {
		called = true
		return nil
	}, func(string) string { return "" })

	if code != 2 || called {
		t.Fatalf("code=%d called=%v", code, called)
	}
}

func TestMainはrunの結果を終了コードにする(t *testing.T) {
	getenv := func(k string) string { return map[string]string{"MYSQL_DSN": "dsn"}[k] }
	cases := map[string]struct {
		err  error
		want int
	}{"成功": {nil, 0}, "失敗": {errors.New("壊れた"), 1}}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			code := app.Main("test", func(context.Context, config.Config) error { return c.err }, getenv)

			if code != c.want {
				t.Fatalf("%d のはずが %d", c.want, code)
			}
		})
	}
}

func TestAPIは注文を受け付けて停止の合図でnilを返す(t *testing.T) {
	cfg, store, _ := fixture(t)
	stop := start(t, app.API, cfg)
	eventually(t, "healthz が200", func() bool {
		r, err := http.Get("http://" + cfg.HTTPAddr + "/healthz")
		if err == nil {
			_ = r.Body.Close()
		}
		return err == nil && r.StatusCode == http.StatusOK
	})

	res, err := http.Post("http://"+cfg.HTTPAddr+"/orders", "application/json",
		strings.NewReader(`{"product_id":"apple","quantity":1,"amount":100}`))
	must(t, err)
	_ = res.Body.Close()

	if res.StatusCode != http.StatusCreated {
		t.Fatalf("201のはずが %d", res.StatusCode)
	}
	recs, err := store.Unpublished(t.Context(), 10)
	must(t, err)
	if len(recs) != 1 {
		t.Fatalf("outbox に1件のはずが %d", len(recs))
	}
	must(t, stop())
}

func TestAPIは待ち受けできないアドレスでエラーを返す(t *testing.T) {
	cfg, _, _ := fixture(t)
	cfg.HTTPAddr = "127.0.0.1:-1"

	if err := app.API(t.Context(), cfg); err == nil {
		t.Fatal("エラーのはず")
	}
}

func TestRelayはoutboxのイベントをパーティションのストリームへ送る(t *testing.T) {
	cfg, store, vk := fixture(t)
	o := event.Order{OrderID: "order-1", ProductID: "apple", Quantity: 1, Amount: 100}
	e, err := event.NewOrderPlaced("e1", time.Now(), o)
	must(t, err)
	must(t, store.PlaceOrder(t.Context(), o, e))
	key := event.StreamKey(cfg.Topic, event.Partition("order-1", cfg.Partitions))

	stop := start(t, app.Relay, cfg)

	eventually(t, key+" に1件", func() bool {
		n, err := vk.Do(t.Context(), vk.B().Xlen().Key(key).Build()).AsInt64()
		return err == nil && n == 1
	})
	must(t, stop())
}

func TestConsumerは読み手の種類に応じて集計を更新する(t *testing.T) {
	cases := map[string]func(*testing.T, *mysqlstore.Store) bool{
		"inventory": func(t *testing.T, s *mysqlstore.Store) bool {
			v, err := s.Stock(t.Context())
			return err == nil && v["apple"] == 2
		},
		"sales": func(t *testing.T, s *mysqlstore.Store) bool {
			v, err := s.Sales(t.Context(), "sales")
			return err == nil && v["apple"] == mysqlstore.Sale{Orders: 1, Amount: 300}
		},
	}
	for handler, updated := range cases {
		t.Run(handler, func(t *testing.T) {
			cfg, store, vk := fixture(t)
			cfg.Handler, cfg.Group, cfg.Consumer = handler, handler, handler+"-1"
			o := event.Order{OrderID: "order-1", ProductID: "apple", Quantity: 2, Amount: 300}
			e, err := event.NewOrderPlaced("e1", time.Now(), o)
			must(t, err)
			key := event.StreamKey(cfg.Topic, event.Partition("order-1", cfg.Partitions))
			_, err = stream.NewValkey(vk, 100).Publish(t.Context(), key, e)
			must(t, err)

			stop := start(t, app.Consumer, cfg)

			eventually(t, handler+" の集計が更新される", func() bool { return updated(t, store) })
			must(t, stop())
		})
	}
}

func Test接続できないときは起動せずエラーを返す(t *testing.T) {
	runs := map[string]app.RunFunc{"API": app.API, "Relay": app.Relay, "Consumer": app.Consumer}
	broken := map[string]func(*config.Config){
		"MySQL":  func(c *config.Config) { c.MySQLDSN = "root@tcp(" + freeAddr(t) + ")/events?timeout=1s" },
		"Valkey": func(c *config.Config) { c.ValkeyAddr = freeAddr(t) },
	}
	for name, run := range runs {
		for target, edit := range broken {
			if name == "API" && target == "Valkey" {
				continue
			}
			t.Run(name+"/"+target, func(t *testing.T) {
				cfg, _, _ := fixture(t)
				edit(&cfg)

				if err := run(t.Context(), cfg); err == nil {
					t.Fatal("エラーのはず")
				}
			})
		}
	}
}
