-- ============================================================================
-- Table: v9_timebasedkeygen_nonhivestyle (MOR avro)
-- Type: MOR (Merge-on-Read)
-- Key Generation: TimestampBasedKeyGenerator (DATE_STRING)
-- Table Version: 9
-- Hudi Version: 1.1.1
-- ============================================================================
-- Features:
--   - Metadata table: ENABLED
--   - Record index: ENABLED
--   - Partitioned: YES (by ts_str, non-hive-style)
--   - Primary key: txn_id
--   - Timestamp type: DATE_STRING
--   - Input format: yyyy-MM-dd'T'HH:mm:ss.SSSZ
--   - Output format: yyyy/MM/dd/HH
--
-- Operations demonstrated:
--   - INSERT (2 batches, with upsert on TXN-001), UPDATE, DELETE, COMPACTION
-- ============================================================================

CREATE TABLE v9_timebasedkeygen_nonhivestyle_mor (
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
    ts_str STRING
) USING HUDI
PARTITIONED BY (ts_str)
TBLPROPERTIES (
    type = 'mor',
    primaryKey = 'txn_id',
    preCombineField = 'txn_ts',
    'hoodie.metadata.enable' = 'true',
    'hoodie.metadata.record.index.enable' = 'true',
    'hoodie.parquet.small.file.limit' = '0',
    'hoodie.clustering.inline' = 'false',
    'hoodie.clustering.async.enabled' = 'false',
    'hoodie.compact.inline' = 'false',
    'hoodie.compact.schedule.inline' = 'false',
    'hoodie.datasource.write.hive_style_partitioning' = 'false',
    'hoodie.table.keygenerator.class' = 'org.apache.hudi.keygen.TimestampBasedKeyGenerator',
    'hoodie.keygen.timebased.timestamp.type' = 'DATE_STRING',
    'hoodie.keygen.timebased.input.dateformat' = "yyyy-MM-dd'T'HH:mm:ss.SSSZ",
    'hoodie.keygen.timebased.output.dateformat' = 'yyyy/MM/dd/HH',
    'hoodie.write.lock.provider' = 'org.apache.hudi.client.transaction.lock.InProcessLockProvider'
);

-- ============================================================================
-- INSERT: Initial 4 records across 4 partitions (creates base parquet files)
-- ============================================================================
INSERT INTO v9_timebasedkeygen_nonhivestyle_mor VALUES
    ('TXN-001', 'ACC-A', 1700000000001, TIMESTAMP '2024-01-15 10:30:00', DATE '2024-01-15',
     1250.00, 'USD', 'debit', 'Amazon', false, 0.00,
     '{"category":"retail","mcc":"5411","risk_score":0.05}',
     '2024-01-15T10:30:00.000Z'),
    ('TXN-002', 'ACC-B', 1700000000002, TIMESTAMP '2024-01-15 11:45:00', DATE '2024-01-15',
     89.99, 'USD', 'debit', 'Netflix', false, 0.00,
     '{"category":"subscription","mcc":"4899","risk_score":0.02}',
     '2024-01-15T11:45:00.000Z'),
    ('TXN-003', 'ACC-C', 1700000000003, TIMESTAMP '2024-01-16 09:00:00', DATE '2024-01-16',
     450.75, 'EUR', 'debit', 'Zalando', false, 0.00,
     '{"category":"retail","mcc":"5651","risk_score":0.03}',
     '2024-01-16T09:00:00.000Z'),
    ('TXN-004', 'ACC-D', 1700000000004, TIMESTAMP '2024-01-17 14:20:00', DATE '2024-01-17',
     5000.00, 'USD', 'transfer', NULL, false, 25.00,
     '{"category":"transfer","mcc":"4829","risk_score":0.15}',
     '2024-01-17T14:20:00.000Z');

-- ============================================================================
-- INSERT: Upsert TXN-001 (mark as reversed) + new TXN-005
-- Writes to avro log files
-- ============================================================================
INSERT INTO v9_timebasedkeygen_nonhivestyle_mor VALUES
    ('TXN-001', 'ACC-A', 1700100000001, TIMESTAMP '2024-01-15 10:30:00', DATE '2024-01-15',
     1250.00, 'USD', 'reversal', 'Amazon', false, 0.00,
     '{"category":"retail","mcc":"5411","risk_score":0.05}',
     '2024-01-15T10:30:00.000Z'),
    ('TXN-005', 'ACC-E', 1700100000005, TIMESTAMP '2024-01-18 16:30:00', DATE '2024-01-18',
     8900.00, 'USD', 'debit', 'Singapore Airlines', true, 45.00,
     '{"category":"travel","mcc":"3000","risk_score":0.12}',
     '2024-01-18T16:30:00.000Z');

-- ============================================================================
-- UPDATE: Modify amount for TXN-003 (writes to avro log file)
-- ============================================================================
UPDATE v9_timebasedkeygen_nonhivestyle_mor
SET amount = 500.00, txn_ts = 1700200000003
WHERE txn_id = 'TXN-003';

-- ============================================================================
-- DELETE: Remove TXN-002 (writes delete marker to avro log file)
-- ============================================================================
DELETE FROM v9_timebasedkeygen_nonhivestyle_mor WHERE txn_id = 'TXN-002';

-- ============================================================================
-- COMPACTION: Merge avro log files with base parquet files
-- ============================================================================
CALL run_compaction(op => 'schedule', table => 'v9_timebasedkeygen_nonhivestyle_mor');

CALL run_compaction(op => 'run', table => 'v9_timebasedkeygen_nonhivestyle_mor');

-- ============================================================================
-- INSERT: Add record after compaction
-- ============================================================================
INSERT INTO v9_timebasedkeygen_nonhivestyle_mor VALUES
    ('TXN-006', 'ACC-F', 1700300000006, TIMESTAMP '2024-01-15 16:00:00', DATE '2024-01-15',
     320.25, 'USD', 'debit', 'Grab', false, 0.00,
     '{"category":"transport","mcc":"4121","risk_score":0.04}',
     '2024-01-15T16:00:00.000Z');
