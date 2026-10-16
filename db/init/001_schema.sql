CREATE TABLE orders (
  order_id     CHAR(36)    NOT NULL PRIMARY KEY,
  product_id   VARCHAR(64) NOT NULL,
  quantity     INT         NOT NULL,
  amount       BIGINT      NOT NULL,
  created_at   DATETIME(6) NOT NULL,
  cancelled_at DATETIME(6) NULL
);

-- 注文と同じトランザクションで書き、relay が古い順にストリームへ送る。
CREATE TABLE outbox (
  seq           BIGINT       NOT NULL AUTO_INCREMENT PRIMARY KEY,
  event_id      CHAR(36)     NOT NULL UNIQUE,
  event_type    VARCHAR(64)  NOT NULL,
  partition_key VARCHAR(64)  NOT NULL,
  payload       JSON         NOT NULL,
  occurred_at   DATETIME(6)  NOT NULL,
  published_at  DATETIME(6)  NULL,
  INDEX idx_unpublished (published_at, seq)
);

-- 読み手ごとに処理済みのイベントを記録し、2回目以降の配信を無視する。
CREATE TABLE processed_events (
  group_name   VARCHAR(64) NOT NULL,
  event_id     CHAR(36)    NOT NULL,
  processed_at DATETIME(6) NOT NULL,
  PRIMARY KEY (group_name, event_id)
);

-- 注文ごとの引当。取消は該当する行を解放するので、確定より先に取消が届くと解放する行がなく、引当が残る。
CREATE TABLE reservations (
  order_id   CHAR(36)                      NOT NULL PRIMARY KEY,
  product_id VARCHAR(64)                   NOT NULL,
  quantity   INT                           NOT NULL,
  status     ENUM('reserved', 'released')  NOT NULL
);

-- projection 列で、同じ集計を別の読み手が作り直した結果を並べて置ける。
CREATE TABLE sales_by_product (
  projection   VARCHAR(64) NOT NULL,
  product_id   VARCHAR(64) NOT NULL,
  order_count  INT         NOT NULL,
  total_amount BIGINT      NOT NULL,
  PRIMARY KEY (projection, product_id)
);
