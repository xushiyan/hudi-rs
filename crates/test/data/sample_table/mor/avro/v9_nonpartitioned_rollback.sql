-- ============================================================================
-- Table: v9_nonpartitioned_rollback (MOR avro)
-- Type: MOR (Merge-on-Read)
-- Key Generation: Simple (single primary key)
-- Table Version: 9
-- Hudi Version: 1.1.1
-- ============================================================================
-- Features:
--   - Metadata table: ENABLED
--   - Record index: ENABLED
--   - Partitioned: NO
--   - Primary key: txn_id
--
-- Operations demonstrated:
--   - INSERT, INSERT, ROLLBACK, INSERT
--
-- NOTE: The rollback_to_instant call requires a specific instant_time.
--       After running the first two INSERTs, query the timeline to find
--       the correct instant_time for the first commit:
--
--       CALL show_commits(table => 'v9_nonpartitioned_rollback_mor', limit => 10);
--
--       Then replace <FIRST_COMMIT_INSTANT_TIME> below with the actual value.
-- ============================================================================

CREATE TABLE v9_nonpartitioned_rollback_mor (
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
    txn_metadata STRING
) USING HUDI
TBLPROPERTIES (
    type = 'mor',
    primaryKey = 'txn_id',
    preCombineField = 'txn_ts',
    'hoodie.metadata.enable' = 'true',
    'hoodie.metadata.record.index.enable' = 'true',
    'hoodie.parquet.small.file.limit' = '0',
    'hoodie.compact.inline' = 'false',
    'hoodie.compact.schedule.inline' = 'false',
    'hoodie.clustering.inline' = 'false',
    'hoodie.clustering.async.enabled' = 'false',
    'hoodie.write.lock.provider' = 'org.apache.hudi.client.transaction.lock.InProcessLockProvider'
);

-- ============================================================================
-- INSERT (commit 1): Initial 3 records
-- ============================================================================
INSERT INTO v9_nonpartitioned_rollback_mor VALUES
    ('TXN-001', 'ACC-A', 1700000000001, TIMESTAMP '2024-01-15 10:30:00', DATE '2024-01-15',
     1250.00, 'USD', 'debit', 'Amazon', false, 0.00,
     '{"category":"retail","mcc":"5411","risk_score":0.05}'),
    ('TXN-002', 'ACC-B', 1700000000002, TIMESTAMP '2024-01-15 11:45:00', DATE '2024-01-15',
     89.99, 'USD', 'debit', 'Netflix', false, 0.00,
     '{"category":"subscription","mcc":"4899","risk_score":0.02}'),
    ('TXN-003', 'ACC-C', 1700000000003, TIMESTAMP '2024-01-16 09:00:00', DATE '2024-01-16',
     450.75, 'EUR', 'debit', 'Zalando', false, 0.00,
     '{"category":"retail","mcc":"5651","risk_score":0.03}');

-- ============================================================================
-- INSERT (commit 2): Upsert TXN-001 + new TXN-004
-- ============================================================================
INSERT INTO v9_nonpartitioned_rollback_mor VALUES
    ('TXN-001', 'ACC-A', 1700100000001, TIMESTAMP '2024-01-15 10:30:00', DATE '2024-01-15',
     1250.00, 'USD', 'reversal', 'Amazon', false, 0.00,
     '{"category":"retail","mcc":"5411","risk_score":0.05}'),
    ('TXN-004', 'ACC-D', 1700100000004, TIMESTAMP '2024-01-17 14:20:00', DATE '2024-01-17',
     5000.00, 'USD', 'transfer', NULL, false, 25.00,
     '{"category":"transfer","mcc":"4829","risk_score":0.15}');

-- ============================================================================
-- ROLLBACK: Roll back to the first commit (undoes commit 2)
-- Run: CALL show_commits(table => 'v9_nonpartitioned_rollback_mor', limit => 10);
-- Find the instant_time of commit 1 and replace below.
-- ============================================================================
CALL rollback_to_instant(table => 'v9_nonpartitioned_rollback_mor', instant_time => '20260403184242950');

-- ============================================================================
-- INSERT (commit 3): Add a record after rollback
-- ============================================================================
INSERT INTO v9_nonpartitioned_rollback_mor VALUES
    ('TXN-002', 'ACC-B', 1700200000002, TIMESTAMP '2024-01-15 11:45:00', DATE '2024-01-15',
     89.99, 'USD', 'debit', 'Netflix', false, 0.00,
     '{"category":"subscription","mcc":"4899","risk_score":0.02}');
