-- Remove exact duplicates before enforcing one runner per market/selection pair.
DELETE duplicate_row
FROM t_selectionid duplicate_row
JOIN t_selectionid keeper
  ON keeper.marketid = duplicate_row.marketid
 AND keeper.selectionid = duplicate_row.selectionid
 AND keeper.id < duplicate_row.id;

SET @runner_index_exists = (
  SELECT COUNT(*)
  FROM information_schema.statistics
  WHERE table_schema = DATABASE()
    AND table_name = 't_selectionid'
    AND index_name = 'uq_selection_market_runner'
);
SET @runner_index_sql = IF(
  @runner_index_exists = 0,
  'ALTER TABLE t_selectionid ADD UNIQUE KEY uq_selection_market_runner (marketid, selectionid)',
  'SELECT 1'
);
PREPARE runner_index_statement FROM @runner_index_sql;
EXECUTE runner_index_statement;
DEALLOCATE PREPARE runner_index_statement;
