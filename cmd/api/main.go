// Command api 注文を受け付け、注文と注文確定イベントを MySQL に書く。
package main

import (
	"os"

	"github.com/okamyuji/event-streaming-valkey/internal/app"
)

func main() {
	os.Exit(app.Main("api", app.API, os.Getenv))
}
