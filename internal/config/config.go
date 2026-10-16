// Package config api・relay・consumer が共通で読む環境変数を解釈する。
package config

import (
	"cmp"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"time"
)

// Config 3つのプロセスの設定をまとめたもの。各プロセスは必要な項目だけを使う。
type Config struct {
	MySQLDSN   string
	ValkeyAddr string
	HTTPAddr   string
	Topic      string
	// Partitions 一度決めたら変えない。変えると同じ注文のイベントが別のパーティションに入り、順序が崩れる。
	Partitions int
	MaxLen     int64
	// Handler consumer が使う読み手の種類。inventory か sales。
	Handler     string
	Group       string
	Consumer    string
	Start       string
	MaxAttempts int
	// RetryDelay 一時的な失敗のあと、同じイベントを処理し直すまで待つ時間。
	RetryDelay time.Duration
	// ReadBlock 新着が無いときに1回の読み込みで待つ時間。
	ReadBlock         time.Duration
	ReadHeaderTimeout time.Duration
	// ReadTimeout 本文を少しずつ送り続ける接続を切るための、要求全体の読み込み時間の上限。
	ReadTimeout     time.Duration
	IdleTimeout     time.Duration
	ShutdownTimeout time.Duration
}

// Load getenv から設定を読み、不正な値はまとめてエラーにする。
func Load(getenv func(string) string) (Config, error) {
	handler := cmp.Or(getenv("HANDLER"), "inventory")
	group := cmp.Or(getenv("GROUP"), handler)
	c := Config{
		MySQLDSN:   getenv("MYSQL_DSN"),
		ValkeyAddr: cmp.Or(getenv("VALKEY_ADDR"), "localhost:6379"),
		HTTPAddr:   cmp.Or(getenv("HTTP_ADDR"), "127.0.0.1:8080"),
		Topic:      cmp.Or(getenv("TOPIC"), "orders"),
		Handler:    handler,
		Group:      group,
		Consumer:   cmp.Or(getenv("CONSUMER"), group+"-1"),
		Start:      cmp.Or(getenv("START"), "0"),

		ReadBlock:         2 * time.Second,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		IdleTimeout:       time.Minute,
		ShutdownTimeout:   10 * time.Second,
	}
	partitions, perr := positive(getenv, "PARTITIONS", 3)
	maxLen, lerr := positive(getenv, "STREAM_MAXLEN", 10000)
	maxAttempts, aerr := positive(getenv, "MAX_ATTEMPTS", 3)
	retryDelay, rerr := duration(getenv, "RETRY_DELAY", time.Second)
	c.Partitions, c.MaxLen, c.MaxAttempts, c.RetryDelay = partitions, int64(maxLen), maxAttempts, retryDelay
	err := errors.Join(perr, lerr, aerr, rerr, c.check())
	return c, err
}

func (c Config) check() error {
	var errs []error
	if c.MySQLDSN == "" {
		errs = append(errs, errors.New("MYSQL_DSN が必要"))
	}
	if !slices.Contains([]string{"inventory", "sales"}, c.Handler) {
		errs = append(errs, fmt.Errorf("HANDLER は inventory か sales: %q", c.Handler))
	}
	if c.Start != "0" && c.Start != "$" {
		errs = append(errs, fmt.Errorf("START は 0 か $: %q", c.Start))
	}
	return errors.Join(errs...)
}

func positive(getenv func(string) string, key string, def int) (int, error) {
	s := getenv(key)
	if s == "" {
		return def, nil
	}
	n, err := strconv.Atoi(s)
	if err != nil || n < 1 {
		return 0, fmt.Errorf("%s は1以上の整数: %q", key, s)
	}
	return n, nil
}

func duration(getenv func(string) string, key string, def time.Duration) (time.Duration, error) {
	s := getenv(key)
	if s == "" {
		return def, nil
	}
	d, err := time.ParseDuration(s)
	if err != nil || d <= 0 {
		return 0, fmt.Errorf("%s は正の時間: %q", key, s)
	}
	return d, nil
}
