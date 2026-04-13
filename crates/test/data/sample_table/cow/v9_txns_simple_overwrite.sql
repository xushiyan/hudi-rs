-- ============================================================================
-- Table: v9_txns_simple_overwrite (COW)
-- Type: COW (Copy-on-Write)
-- Key Generation: Simple (single primary key)
-- Table Version: 9
-- Hudi Version: 1.1.1
-- ============================================================================
-- Features:
--   - Metadata table: ENABLED
--   - Record index: ENABLED
--   - Partitioned: YES (by region, non-hive-style)
--   - Primary key: txn_id
--
-- Operations demonstrated:
--   - INSERT (2 batches), INSERT OVERWRITE TABLE (full table overwrite)
-- ============================================================================

CREATE TABLE v9_txns_cow_simple_overwrite (
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
    region STRING
) USING HUDI
PARTITIONED BY (region)
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
    'hoodie.write.lock.provider' = 'org.apache.hudi.client.transaction.lock.InProcessLockProvider'
);

-- ============================================================================
-- INSERT: Initial 6 records across 3 partitions (us, eu, apac)
-- ============================================================================
INSERT INTO v9_txns_cow_simple_overwrite VALUES
    ('TXN-001', 'ACC-A', 1700000000001, TIMESTAMP '2024-01-15 10:30:00', DATE '2024-01-15',
     1250.00, 'USD', 'debit', 'Amazon', false, 0.00,
     '{"category":"retail","mcc":"5411","risk_score":0.05}', 'us'),
    ('TXN-002', 'ACC-B', 1700000000002, TIMESTAMP '2024-01-15 11:45:00', DATE '2024-01-15',
     89.99, 'USD', 'debit', 'Netflix', false, 0.00,
     '{"category":"subscription","mcc":"4899","risk_score":0.02}', 'us'),
    ('TXN-003', 'ACC-C', 1700000000003, TIMESTAMP '2024-01-15 09:00:00', DATE '2024-01-15',
     450.75, 'EUR', 'debit', 'Zalando', false, 0.00,
     '{"category":"retail","mcc":"5651","risk_score":0.03}', 'eu'),
    ('TXN-004', 'ACC-D', 1700000000004, TIMESTAMP '2024-01-15 16:30:00', DATE '2024-01-15',
     2100.00, 'GBP', 'credit', 'Salary Deposit', false, 0.00,
     '{"category":"income","mcc":"6011","risk_score":0.01}', 'eu'),
    ('TXN-005', 'ACC-E', 1700000000005, TIMESTAMP '2024-01-16 02:15:00', DATE '2024-01-16',
     8900.00, 'USD', 'debit', 'Singapore Airlines', true, 45.00,
     '{"category":"travel","mcc":"3000","risk_score":0.12}', 'apac'),
    ('TXN-006', 'ACC-F', 1700000000006, TIMESTAMP '2024-01-16 08:00:00', DATE '2024-01-16',
     320.25, 'USD', 'debit', 'Grab', false, 0.00,
     '{"category":"transport","mcc":"4121","risk_score":0.04}', 'apac');

-- ============================================================================
-- INSERT: 2 more records to us and eu partitions
-- ============================================================================
INSERT INTO v9_txns_cow_simple_overwrite VALUES
    ('TXN-007', 'ACC-G', 1700100000007, TIMESTAMP '2024-01-17 10:00:00', DATE '2024-01-17',
     750.00, 'USD', 'debit', 'Best Buy', false, 0.00,
     '{"category":"electronics","mcc":"5732","risk_score":0.05}', 'us'),
    ('TXN-008', 'ACC-H', 1700100000008, TIMESTAMP '2024-01-17 11:30:00', DATE '2024-01-17',
     1500.00, 'EUR', 'debit', 'IKEA', false, 0.00,
     '{"category":"retail","mcc":"5712","risk_score":0.04}', 'eu');

-- ============================================================================
-- INSERT OVERWRITE TABLE: Replace all data in all partitions
-- ============================================================================
INSERT OVERWRITE TABLE v9_txns_cow_simple_overwrite SELECT
    'TXN-101', 'ACC-X', CAST(1700500000001 AS BIGINT), TIMESTAMP '2024-01-18 09:00:00', DATE '2024-01-18',
    CAST(999.99 AS DECIMAL(15,2)), 'USD', 'debit', 'Apple Store', false, CAST(0.00 AS DECIMAL(10,2)),
    '{"category":"electronics","mcc":"5732","risk_score":0.04}', 'us'
UNION ALL SELECT
    'TXN-102', 'ACC-Y', CAST(1700500000002 AS BIGINT), TIMESTAMP '2024-01-18 14:00:00', DATE '2024-01-18',
    CAST(350.00 AS DECIMAL(15,2)), 'EUR', 'debit', 'MediaMarkt', false, CAST(0.00 AS DECIMAL(10,2)),
    '{"category":"electronics","mcc":"5732","risk_score":0.06}', 'eu'
UNION ALL SELECT
    'TXN-103', 'ACC-Z', CAST(1700500000003 AS BIGINT), TIMESTAMP '2024-01-18 16:00:00', DATE '2024-01-18',
    CAST(4500.00 AS DECIMAL(15,2)), 'USD', 'debit', 'Japan Airlines', true, CAST(50.00 AS DECIMAL(10,2)),
    '{"category":"travel","mcc":"3000","risk_score":0.11}', 'apac';
