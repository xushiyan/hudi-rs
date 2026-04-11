-- ============================================================================
-- Table: v9_timebasedkeygen_unixtimestamp (COW)
-- Type: COW (Copy-on-Write)
-- Key Generation: TimestampBasedKeyGenerator (UNIX_TIMESTAMP)
-- Table Version: 9
-- Hudi Version: 1.1.1
-- ============================================================================
-- Features:
--   - Metadata table: ENABLED
--   - Record index: ENABLED
--   - Partitioned: YES (by ts_epoch, non-hive-style)
--   - Primary key: txn_id
--   - Timestamp type: UNIX_TIMESTAMP (seconds since epoch)
--   - Output format: yyyy/MM/dd
--
-- Operations demonstrated:
--   - INSERT (2 batches), UPDATE, DELETE
-- ============================================================================

CREATE TABLE v9_timebasedkeygen_unixtimestamp_cow (
    txn_id STRING,
    account_id STRING,
    txn_ts BIGINT,
    txn_datetime TIMESTAMP,
    txn_date DATE,
    amount DECIMAL(15,2),
    currency STRING,
    txn_type STRING,
    merchant_name STRING,
    is_international BOOLEAN,
    fee_amount DECIMAL(10,2),
    txn_metadata STRING,
    ts_epoch LONG
) USING HUDI
PARTITIONED BY (ts_epoch)
TBLPROPERTIES (
    type = 'cow',
    primaryKey = 'txn_id',
    preCombineField = 'txn_ts',
    'hoodie.metadata.enable' = 'true',
    'hoodie.metadata.record.index.enable' = 'true',
    'hoodie.parquet.small.file.limit' = '0',
    'hoodie.clustering.inline' = 'false',
    'hoodie.clustering.async.enabled' = 'false',
    'hoodie.datasource.write.hive_style_partitioning' = 'false',
    'hoodie.table.keygenerator.class' = 'org.apache.hudi.keygen.TimestampBasedKeyGenerator',
    'hoodie.keygen.timebased.timestamp.type' = 'UNIX_TIMESTAMP',
    'hoodie.keygen.timebased.output.dateformat' = 'yyyy/MM/dd',
    'hoodie.write.lock.provider' = 'org.apache.hudi.client.transaction.lock.InProcessLockProvider'
);

-- ============================================================================
-- INSERT: Initial 4 records across 3 day-partitions
-- Epoch seconds:
--   1705311000 = 2024-01-15 10:30:00 UTC
--   1705315500 = 2024-01-15 11:45:00 UTC
--   1705367700 = 2024-01-16 02:15:00 UTC
--   1705397400 = 2024-01-16 10:30:00 UTC
-- ============================================================================
INSERT INTO v9_timebasedkeygen_unixtimestamp_cow VALUES
    ('TXN-001', 'ACC-A', 1700000000001, TIMESTAMP '2024-01-15 10:30:00', DATE '2024-01-15',
     1250.00, 'USD', 'debit', 'Amazon', false, 0.00,
     '{"category":"retail","mcc":"5411","risk_score":0.05}', 1705311000),
    ('TXN-002', 'ACC-B', 1700000000002, TIMESTAMP '2024-01-15 11:45:00', DATE '2024-01-15',
     89.99, 'USD', 'debit', 'Netflix', false, 0.00,
     '{"category":"subscription","mcc":"4899","risk_score":0.02}', 1705315500),
    ('TXN-003', 'ACC-C', 1700000000003, TIMESTAMP '2024-01-16 09:00:00', DATE '2024-01-16',
     450.75, 'EUR', 'debit', 'Zalando', false, 0.00,
     '{"category":"retail","mcc":"5651","risk_score":0.03}', 1705367700),
    ('TXN-004', 'ACC-D', 1700000000004, TIMESTAMP '2024-01-16 10:30:00', DATE '2024-01-16',
     5000.00, 'USD', 'transfer', NULL, false, 25.00,
     '{"category":"transfer","mcc":"4829","risk_score":0.15}', 1705397400);

-- ============================================================================
-- INSERT: Upsert TXN-001 + new TXN-005, TXN-006
-- Epoch seconds:
--   1705485600 = 2024-01-17 10:00:00 UTC
--   1705489200 = 2024-01-17 11:00:00 UTC
-- ============================================================================
INSERT INTO v9_timebasedkeygen_unixtimestamp_cow VALUES
    ('TXN-001', 'ACC-A', 1700100000001, TIMESTAMP '2024-01-15 10:30:00', DATE '2024-01-15',
     1250.00, 'USD', 'reversal', 'Amazon', false, 0.00,
     '{"category":"retail","mcc":"5411","risk_score":0.05}', 1705311000),
    ('TXN-005', 'ACC-E', 1700100000005, TIMESTAMP '2024-01-17 10:00:00', DATE '2024-01-17',
     1500.00, 'EUR', 'debit', 'IKEA', false, 0.00,
     '{"category":"retail","mcc":"5712","risk_score":0.04}', 1705485600),
    ('TXN-006', 'ACC-F', 1700100000006, TIMESTAMP '2024-01-17 11:00:00', DATE '2024-01-17',
     2200.00, 'EUR', 'transfer', NULL, false, 15.00,
     '{"category":"transfer","mcc":"4829","risk_score":0.10}', 1705489200);

-- ============================================================================
-- UPDATE: Modify amount for TXN-003
-- ============================================================================
UPDATE v9_timebasedkeygen_unixtimestamp_cow
SET amount = 500.00, txn_ts = 1700200000003
WHERE txn_id = 'TXN-003';

-- ============================================================================
-- DELETE: Remove TXN-004
-- ============================================================================
DELETE FROM v9_timebasedkeygen_unixtimestamp_cow WHERE txn_id = 'TXN-004';
