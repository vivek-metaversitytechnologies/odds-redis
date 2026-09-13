-- Non-unique prefixes fit MyISAM's 1000-byte key limit with utf8mb4 and
-- preserve existing settlement history. Do not convert table engines here.
SET @regular_result_lookup_exists = (
  SELECT COUNT(*) FROM information_schema.statistics
  WHERE table_schema = DATABASE() AND table_name = 't_matchresult'
    AND column_name = 'marketid' AND seq_in_index = 1
    AND (sub_part IS NULL OR sub_part >= 191)
);
SET @regular_result_lookup_sql = IF(
  @regular_result_lookup_exists = 0,
  'ALTER TABLE t_matchresult ADD INDEX idx_matchresult_market_selection (marketid(191),selectionid)',
  'SELECT 1'
);
PREPARE regular_result_lookup_statement FROM @regular_result_lookup_sql;
EXECUTE regular_result_lookup_statement;
DEALLOCATE PREPARE regular_result_lookup_statement;

SET @exceptional_result_table_exists = (
  SELECT COUNT(*) FROM information_schema.tables
  WHERE table_schema = DATABASE() AND table_name = 't_matchabondendtie'
);
SET @exceptional_result_lookup_exists = (
  SELECT COUNT(*) FROM information_schema.statistics
  WHERE table_schema = DATABASE() AND table_name = 't_matchabondendtie'
    AND column_name = 'marketid' AND seq_in_index = 1
    AND (sub_part IS NULL OR sub_part >= 191)
);
SET @exceptional_result_lookup_sql = IF(
  @exceptional_result_table_exists > 0 AND @exceptional_result_lookup_exists = 0,
  'ALTER TABLE t_matchabondendtie ADD INDEX idx_exceptional_result_market (marketid(191))',
  'SELECT 1'
);
PREPARE exceptional_result_lookup_statement FROM @exceptional_result_lookup_sql;
EXECUTE exceptional_result_lookup_statement;
DEALLOCATE PREPARE exceptional_result_lookup_statement;
