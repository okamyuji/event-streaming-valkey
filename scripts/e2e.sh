#!/usr/bin/env bash
# compose で全体を起動し、注文から集計までの流れと、障害時のふるまいを HTTP と CLI で確かめる。
set -euo pipefail
cd "$(dirname "$0")/.."

API=http://127.0.0.1:8080
# KEEP=1 なら終了後も環境を残し、ログや中身を調べられるようにする。
trap '[ -n "${KEEP:-}" ] || docker compose --profile replay down -v >/dev/null 2>&1' EXIT

step() { printf '\n== %s\n' "$*"; }

# eventually <説明> <期待値> <コマンド...> 期待値になるまで最大30秒待つ。
eventually() {
  local what=$1 want=$2 got=""
  shift 2
  for _ in $(seq 60); do
    got=$("$@" 2>/dev/null || true)
    if [ "$got" = "$want" ]; then
      echo "ok: $what = $got"
      return 0
    fi
    sleep 0.5
  done
  echo "NG: $what は $want のはずが $got" >&2
  exit 1
}

order() { place "$@" >/dev/null; }
stock() { curl -sf "$API/stock" | jq -cS .; }
sales() { curl -sf "$API/sales?projection=$1" | jq -cS .; }
xlen() { docker compose exec -T valkey valkey-cli XLEN "$1" | tr -d '\r'; }
stream_total() { echo $(($(xlen orders.p0) + $(xlen orders.p1) + $(xlen orders.p2))); }
processed() { docker compose exec -T mysql mysql -uroot -N events -e "SELECT COUNT(*) FROM processed_events WHERE group_name = '$1'"; }
outbox_rows() { docker compose exec -T mysql mysql -uroot -N events -e "SELECT COUNT(*) FROM outbox"; }
place() {
  curl -sf -X POST "$API/orders" -H 'Content-Type: application/json' \
    -d "{\"product_id\":\"$1\",\"quantity\":$2,\"amount\":$3}" | jq -r .order_id
}
cancel() { curl -sf -X POST "$API/orders/$1/cancel" >/dev/null; }
# inject <イベントID> <種別> <注文ID> <数量> <金額> relay を通さずに orders.p0 へ直接書く。
inject() {
  docker compose exec -T valkey valkey-cli XADD orders.p0 '*' event_id "$1" type "$2" key "$3" \
    occurred_at 2026-10-16T00:00:00Z payload "{\"order_id\":\"$3\",\"product_id\":\"cherry\",\"quantity\":$4,\"amount\":$5}" >/dev/null
}
unpublished() { docker compose exec -T mysql mysql -uroot -N events -e "SELECT COUNT(*) FROM outbox WHERE published_at IS NULL"; }

step "起動"
docker compose up -d --build --wait mysql valkey
docker compose up -d --build api relay inventory sales
eventually "api の healthz" 200 curl -s -o /dev/null -w '%{http_code}' "$API/healthz"

step "別のサイトを開いたブラウザからの書き込みと、上限を超える注文は受け付けない"
eventually "別オリジンからの POST" 403 curl -s -o /dev/null -w '%{http_code}' -X POST "$API/orders" \
  -H 'Sec-Fetch-Site: cross-site' -d '{"product_id":"apple","quantity":1,"amount":100}'
eventually "ループバックではない Host" 403 curl -s -o /dev/null -w '%{http_code}' -H 'Host: evil.example' "$API/stock"
eventually "上限を超える金額" 400 curl -s -o /dev/null -w '%{http_code}' -X POST "$API/orders" \
  -d '{"product_id":"apple","quantity":1,"amount":9223372036854775807}'

step "注文6件を2つの読み手がそれぞれ集計する"
for _ in 1 2 3; do
  order apple 1 100
  order banana 2 300
done
eventually "在庫の引当" '{"apple":3,"banana":6}' stock
eventually "売上" '{"apple":{"amount":300,"orders":3},"banana":{"amount":900,"orders":3}}' sales sales

step "確定の直後に取り消した注文は、同じパーティションで確定→取消の順に処理され、引当が残らない"
for _ in 1 2; do
  cancel "$(place apple 1 100)"
done
eventually "在庫の読み手が処理した件数（確定8＋取消2）" 10 processed inventory
eventually "在庫の引当" '{"apple":3,"banana":6}' stock
eventually "売上の読み手が処理した件数" 10 processed sales
eventually "売上" '{"apple":{"amount":300,"orders":3},"banana":{"amount":900,"orders":3}}' sales sales

step "売上の読み手を止めても、在庫の読み手は進み続ける"
docker compose stop sales
order apple 1 100
order apple 1 100
order banana 2 300
eventually "在庫の引当" '{"apple":5,"banana":8}' stock
eventually "止まっている売上" '{"apple":{"amount":300,"orders":3},"banana":{"amount":900,"orders":3}}' sales sales

step "再開した売上の読み手は、止まっていた間の分から追いつく"
docker compose start sales
eventually "売上" '{"apple":{"amount":500,"orders":5},"banana":{"amount":1200,"orders":4}}' sales sales

step "MySQL が止まると読み手はその場で再試行し、再起動しても未ACK分を引き取って順番どおりに片付ける"
docker compose stop mysql
inject inject-x-placed OrderPlaced order-x 1 10
inject inject-x-cancelled OrderCancelled order-x 1 10
inject inject-y-placed OrderPlaced order-y 2 20
eventually "在庫の読み手が再試行している" yes sh -c 'docker compose logs inventory | grep -q "待ってやり直す" && echo yes'
docker compose restart inventory
docker compose up -d --wait mysql
eventually "在庫の引当（order-x は確定→取消で残らない）" '{"apple":5,"banana":8,"cherry":2}' stock
eventually "売上" '{"apple":{"amount":500,"orders":5},"banana":{"amount":1200,"orders":4},"cherry":{"amount":20,"orders":1}}' sales sales
eventually "DLQ は空" 0 xlen orders.dlq

step "relay が同じイベントをもう一度送っても、集計は二重にならない"
before=$(stream_total)
rows=$(outbox_rows)
docker compose exec -T mysql mysql -uroot events -e "UPDATE outbox SET published_at = NULL"
eventually "未送信の残り" 0 unpublished
eventually "ストリームの合計件数（outbox の行数ぶん増える）" $((before + rows)) stream_total
eventually "在庫の引当" '{"apple":5,"banana":8,"cherry":2}' stock
eventually "売上" '{"apple":{"amount":500,"orders":5},"banana":{"amount":1200,"orders":4},"cherry":{"amount":20,"orders":1}}' sales sales

step "壊れたイベントは DLQ に退避され、後続は止まらない"
docker compose exec -T valkey valkey-cli XADD orders.p0 '*' event_id broken-1 type OrderPlaced key k \
  occurred_at 2026-10-16T00:00:00Z payload '{broken' >/dev/null
order banana 2 300
eventually "DLQ（在庫と売上の読み手が1件ずつ）" 2 xlen orders.dlq
eventually "在庫の引当" '{"apple":5,"banana":10,"cherry":2}' stock
eventually "売上" '{"apple":{"amount":500,"orders":5},"banana":{"amount":1500,"orders":5},"cherry":{"amount":20,"orders":1}}' sales sales

step "後から加えた読み手が、ストリームの先頭から売上集計を作り直す"
docker compose --profile replay up -d --build sales-replay
eventually "作り直した売上" "$(sales sales)" sales sales-replay
eventually "DLQ（作り直しの読み手も1件）" 3 xlen orders.dlq

printf '\nすべての確認が通った\n'
