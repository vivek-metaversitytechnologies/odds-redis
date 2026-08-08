-- Build a temporary supporting index before duplicate cleanup. This avoids the
-- unindexed self-join in 001 becoming quadratic on a large production table.
SET @runner_unique_exists = (
  SELECT COUNT(*) FROM information_schema.statistics
  WHERE table_schema = DATABASE()
    AND table_name = 't_selectionid'
    AND index_name = 'uq_selection_market_runner'
);
SET @runner_helper_exists = (
  SELECT COUNT(*) FROM information_schema.statistics
  WHERE table_schema = DATABASE()
    AND table_name = 't_selectionid'
    AND index_name = 'idx_selection_runner_dedupe'
);
SET @runner_helper_sql = IF(
  @runner_unique_exists = 0 AND @runner_helper_exists = 0,
  'ALTER TABLE t_selectionid ADD INDEX idx_selection_runner_dedupe (marketid(64), id)',
  'SELECT 1'
);
PREPARE runner_helper_statement FROM @runner_helper_sql;
EXECUTE runner_helper_statement;
DEALLOCATE PREPARE runner_helper_statement;

DELETE duplicate_row
FROM t_selectionid duplicate_row
JOIN t_selectionid keeper
  ON keeper.marketid = duplicate_row.marketid
 AND keeper.selectionid = duplicate_row.selectionid
 AND keeper.id < duplicate_row.id
WHERE @runner_unique_exists = 0;

SET @runner_hash_column_exists = (
  SELECT COUNT(*) FROM information_schema.columns
  WHERE table_schema = DATABASE()
    AND table_name = 't_selectionid'
    AND column_name = 'market_runner_hash'
);
SET @runner_hash_column_sql = IF(
  @runner_hash_column_exists = 0,
  IF(
    LOCATE('MariaDB', VERSION()) > 0,
    'ALTER TABLE t_selectionid ADD COLUMN market_runner_hash BINARY(32) AS (UNHEX(SHA2(CONCAT(COALESCE(CAST(marketid AS CHAR), ''''), CHAR(0), COALESCE(CAST(selectionid AS CHAR), '''')), 256))) PERSISTENT',
    'ALTER TABLE t_selectionid ADD COLUMN market_runner_hash BINARY(32) GENERATED ALWAYS AS (UNHEX(SHA2(CONCAT(COALESCE(CAST(marketid AS CHAR), ''''), CHAR(0), COALESCE(CAST(selectionid AS CHAR), '''')), 256))) STORED'
  ),
  'SELECT 1'
);
PREPARE runner_hash_column_statement FROM @runner_hash_column_sql;
EXECUTE runner_hash_column_statement;
DEALLOCATE PREPARE runner_hash_column_statement;

SET @runner_unique_sql = IF(
  @runner_unique_exists = 0,
  'ALTER TABLE t_selectionid ADD UNIQUE KEY uq_selection_market_runner (market_runner_hash)',
  'SELECT 1'
);
PREPARE runner_unique_statement FROM @runner_unique_sql;
EXECUTE runner_unique_statement;
DEALLOCATE PREPARE runner_unique_statement;

SET @runner_helper_exists = (
  SELECT COUNT(*) FROM information_schema.statistics
  WHERE table_schema = DATABASE()
    AND table_name = 't_selectionid'
    AND index_name = 'idx_selection_runner_dedupe'
);
SET @runner_helper_drop_sql = IF(
  @runner_helper_exists > 0,
  'ALTER TABLE t_selectionid DROP INDEX idx_selection_runner_dedupe',
  'SELECT 1'
);
PREPARE runner_helper_drop_statement FROM @runner_helper_drop_sql;
EXECUTE runner_helper_drop_statement;
DEALLOCATE PREPARE runner_helper_drop_statement;
