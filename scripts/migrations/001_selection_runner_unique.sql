SET @runner_index_exists = (
  SELECT COUNT(*)
  FROM information_schema.statistics
  WHERE table_schema = DATABASE()
    AND table_name = 't_selectionid'
    AND index_name = 'uq_selection_market_runner'
);
SET @runner_cleanup_sql = IF(
  @runner_index_exists = 0,
  'DELETE duplicate_row FROM t_selectionid duplicate_row JOIN t_selectionid keeper ON keeper.marketid=duplicate_row.marketid AND keeper.selectionid=duplicate_row.selectionid AND keeper.id<duplicate_row.id',
  'SELECT 1'
);
PREPARE runner_cleanup_statement FROM @runner_cleanup_sql;
EXECUTE runner_cleanup_statement;
DEALLOCATE PREPARE runner_cleanup_statement;
SET @runner_index_sql = IF(
  @runner_index_exists = 0,
  'ALTER TABLE t_selectionid ADD UNIQUE KEY uq_selection_market_runner (market_runner_hash)',
  'SELECT 1'
);
PREPARE runner_index_statement FROM @runner_index_sql;
EXECUTE runner_index_statement;
DEALLOCATE PREPARE runner_index_statement;
