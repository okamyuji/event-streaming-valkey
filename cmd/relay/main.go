// Command relay outbox の未送信イベントを Valkey Streams へ送る。
package main

import (
	"os"

	"github.com/okamyuji/event-streaming-valkey/internal/app"
)

func main() {
	os.Exit(app.Main("relay", app.Relay, os.Getenv))
}
