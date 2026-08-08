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
  'ALTER TABLE t_selectionid ADD INDEX idx_selection_runner_dedupe (marketid, selectionid, id)',
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

SET @runner_unique_sql = IF(
  @runner_unique_exists = 0,
  'ALTER TABLE t_selectionid ADD UNIQUE KEY uq_selection_market_runner (marketid, selectionid)',
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
