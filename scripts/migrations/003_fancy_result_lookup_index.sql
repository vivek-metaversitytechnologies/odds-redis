-- Result lookups and name repair use fancyid. Keep this non-unique: existing
-- result history may contain multiple rows per market. Do not change its engine.
SET @fancy_result_lookup_exists = (
  SELECT COUNT(*) FROM information_schema.statistics
  WHERE table_schema = DATABASE()
    AND table_name = 't_fancyresult'
    AND column_name = 'fancyid' AND seq_in_index = 1 AND sub_part IS NULL
);
SET @fancy_result_lookup_sql = IF(
  @fancy_result_lookup_exists = 0,
  'ALTER TABLE t_fancyresult ADD INDEX idx_fancyresult_fancyid (fancyid)',
  'SELECT 1'
);
PREPARE fancy_result_lookup_statement FROM @fancy_result_lookup_sql;
EXECUTE fancy_result_lookup_statement;
DEALLOCATE PREPARE fancy_result_lookup_statement;
