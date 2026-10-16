// Command consumer コンシューマグループとしてストリームを読み、読み手の集計を更新する。
package main

import (
	"os"

	"github.com/okamyuji/event-streaming-valkey/internal/app"
)

func main() {
	os.Exit(app.Main("consumer", app.Consumer, os.Getenv))
}
