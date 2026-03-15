-- Hypercore Indexer Schema

CREATE TABLE IF NOT EXISTS blocks (
    block_number    BIGINT PRIMARY KEY,
    block_hash      BYTEA NOT NULL,
    parent_hash     BYTEA NOT NULL,
    timestamp       BIGINT NOT NULL,
    gas_used        BIGINT NOT NULL,
    gas_limit       BIGINT NOT NULL,
    base_fee_per_gas BIGINT,
    tx_count        INTEGER NOT NULL,
    system_tx_count INTEGER NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_blocks_timestamp ON blocks (timestamp);

CREATE TABLE IF NOT EXISTS transactions (
    block_number    BIGINT NOT NULL REFERENCES blocks(block_number),
    tx_index        INTEGER NOT NULL,
    tx_hash         BYTEA NOT NULL,
    tx_type         SMALLINT NOT NULL,
    "from"          BYTEA,
    "to"            BYTEA,
    value           NUMERIC NOT NULL,
    input           BYTEA NOT NULL,
    gas_limit       BIGINT NOT NULL,
    gas_used        BIGINT NOT NULL,
    success         BOOLEAN NOT NULL,
    PRIMARY KEY (block_number, tx_index)
);

CREATE INDEX IF NOT EXISTS idx_transactions_hash ON transactions (tx_hash);
CREATE INDEX IF NOT EXISTS idx_transactions_from ON transactions ("from") WHERE "from" IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_transactions_to ON transactions ("to") WHERE "to" IS NOT NULL;

CREATE TABLE IF NOT EXISTS system_transfers (
    block_number    BIGINT NOT NULL REFERENCES blocks(block_number),
    tx_index        INTEGER NOT NULL,
    official_hash   BYTEA NOT NULL,
    explorer_hash   BYTEA NOT NULL,
    system_address  BYTEA NOT NULL,
    asset_type      TEXT NOT NULL,
    asset_index     SMALLINT,
    recipient       BYTEA NOT NULL,
    amount_wei      NUMERIC NOT NULL,
    PRIMARY KEY (block_number, tx_index)
);

CREATE INDEX IF NOT EXISTS idx_system_transfers_recipient ON system_transfers (recipient);
CREATE INDEX IF NOT EXISTS idx_system_transfers_official ON system_transfers (official_hash);
CREATE INDEX IF NOT EXISTS idx_system_transfers_explorer ON system_transfers (explorer_hash);

CREATE TABLE IF NOT EXISTS event_logs (
    block_number    BIGINT NOT NULL,
    tx_index        INTEGER NOT NULL,
    log_index       INTEGER NOT NULL,
    address         BYTEA NOT NULL,
    topic0          BYTEA,
    topic1          BYTEA,
    topic2          BYTEA,
    topic3          BYTEA,
    data            BYTEA NOT NULL,
    PRIMARY KEY (block_number, tx_index, log_index),
    FOREIGN KEY (block_number, tx_index) REFERENCES transactions(block_number, tx_index)
);

CREATE INDEX IF NOT EXISTS idx_event_logs_address_topic0 ON event_logs (address, topic0);
CREATE INDEX IF NOT EXISTS idx_event_logs_topic0 ON event_logs (topic0);

CREATE TABLE IF NOT EXISTS indexer_cursor (
    network         TEXT PRIMARY KEY,
    last_block      BIGINT NOT NULL,
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
