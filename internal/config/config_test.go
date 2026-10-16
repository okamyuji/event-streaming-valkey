package config_test

import (
	"testing"
	"time"

	"github.com/okamyuji/event-streaming-valkey/internal/config"
)

func env(m map[string]string) func(string) string {
	return func(k string) string { return m[k] }
}

func TestLoadは未設定の項目に既定値を入れる(t *testing.T) {
	c, err := config.Load(env(map[string]string{"MYSQL_DSN": "dsn"}))

	if err != nil {
		t.Fatal(err)
	}
	want := config.Config{
		MySQLDSN: "dsn", ValkeyAddr: "localhost:6379", HTTPAddr: "127.0.0.1:8080",
		Topic: "orders", Partitions: 3, MaxLen: 10000,
		Handler: "inventory", Group: "inventory", Consumer: "inventory-1", Start: "0",
		MaxAttempts: 3, RetryDelay: time.Second,
		ReadBlock: 2 * time.Second, ReadHeaderTimeout: 5 * time.Second, ShutdownTimeout: 10 * time.Second,
		ReadTimeout: 10 * time.Second, IdleTimeout: time.Minute,
	}
	if c != want {
		t.Fatalf("\n got=%+v\nwant=%+v", c, want)
	}
}

func TestLoadは指定された値を使いグループ名の既定は読み手の種類にする(t *testing.T) {
	c, err := config.Load(env(map[string]string{
		"MYSQL_DSN": "dsn", "VALKEY_ADDR": "valkey:6379", "HTTP_ADDR": ":9000",
		"TOPIC": "t", "PARTITIONS": "1", "STREAM_MAXLEN": "5",
		"HANDLER": "sales", "CONSUMER": "sales-2", "START": "$",
		"MAX_ATTEMPTS": "1", "RETRY_DELAY": "1s",
	}))

	if err != nil {
		t.Fatal(err)
	}
	want := config.Config{
		MySQLDSN: "dsn", ValkeyAddr: "valkey:6379", HTTPAddr: ":9000",
		Topic: "t", Partitions: 1, MaxLen: 5,
		Handler: "sales", Group: "sales", Consumer: "sales-2", Start: "$",
		MaxAttempts: 1, RetryDelay: time.Second,
		ReadBlock: 2 * time.Second, ReadHeaderTimeout: 5 * time.Second, ShutdownTimeout: 10 * time.Second,
		ReadTimeout: 10 * time.Second, IdleTimeout: time.Minute,
	}
	if c != want {
		t.Fatalf("\n got=%+v\nwant=%+v", c, want)
	}
}

func TestLoadはGROUPを指定すると読み手の種類と別の名前にできる(t *testing.T) {
	c, err := config.Load(env(map[string]string{"MYSQL_DSN": "dsn", "HANDLER": "sales", "GROUP": "sales-replay"}))

	if err != nil || c.Group != "sales-replay" || c.Consumer != "sales-replay-1" {
		t.Fatalf("group=%q consumer=%q err=%v", c.Group, c.Consumer, err)
	}
}

func TestLoadは不正な値を拒む(t *testing.T) {
	cases := map[string]map[string]string{
		"DSNがない":        {},
		"分割数が0":         {"PARTITIONS": "0"},
		"分割数が数でない":      {"PARTITIONS": "three"},
		"保持件数が0":        {"STREAM_MAXLEN": "0"},
		"保持件数が数でない":     {"STREAM_MAXLEN": "many"},
		"試行上限が0":        {"MAX_ATTEMPTS": "0"},
		"試行上限が数でない":     {"MAX_ATTEMPTS": "x"},
		"再試行の間隔が0":      {"RETRY_DELAY": "0s"},
		"再試行の間隔が読めない":   {"RETRY_DELAY": "soon"},
		"読み手の種類が不明":     {"HANDLER": "billing"},
		"開始位置が0でも$でもない": {"START": "latest"},
	}
	for name, m := range cases {
		t.Run(name, func(t *testing.T) {
			if name != "DSNがない" {
				m["MYSQL_DSN"] = "dsn"
			}
			if _, err := config.Load(env(m)); err == nil {
				t.Fatal("エラーのはず")
			}
		})
	}
}

func TestLoadは境界値の1を受け付ける(t *testing.T) {
	_, err := config.Load(env(map[string]string{
		"MYSQL_DSN": "dsn", "PARTITIONS": "1", "STREAM_MAXLEN": "1", "MAX_ATTEMPTS": "1", "RETRY_DELAY": "1ms",
	}))
	if err != nil {
		t.Fatal(err)
	}
}
