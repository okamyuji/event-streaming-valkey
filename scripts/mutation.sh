#!/usr/bin/env bash
# gremlins の結果を読み、KILLED と NOT VIABLE（変異後にビルドできないもの）以外が1件でもあれば失敗する。
# gremlins 0.5.1 の --threshold-* は指定した値が判定に届かず常に通るため、判定はここで行う。
set -euo pipefail
cd "$(dirname "$0")/.."

GREMLINS="${GREMLINS:-go run github.com/go-gremlins/gremlins/cmd/gremlins@v0.5.1}"
OUT=gremlins.json

# テスト側で締め切りを設けているので、gremlins の時間切れには余裕を持たせて KILLED として数えさせる。
# shellcheck disable=SC2086
$GREMLINS unleash --timeout-coefficient 10 -o "$OUT" .

survivors=$(jq -r '
  .files[] | .file_name as $f | .mutations[]
  | select(.status != "KILLED" and .status != "NOT VIABLE")
  | "\(.status) \(.type) \($f):\(.line):\(.column)"' "$OUT")
jq -r '"ミュータント \(.mutants_total) 件 / KILLED \(.mutants_killed) 件 / LIVED \(.mutants_lived) 件 / NOT COVERED \(.mutants_not_covered) 件"' "$OUT"
if [ -n "$survivors" ]; then
  echo "KILLED にならなかったミュータント:"
  echo "$survivors"
  exit 1
fi
echo "すべてのミュータントが KILLED になった"
