-- Support event-first active-match reads and the same event/market/result lookups
-- used by discovery, subscription reconciliation and settlement jobs.
SET @event_active_index_exists = (
  SELECT COUNT(*) FROM information_schema.statistics
  WHERE table_schema = DATABASE()
    AND table_name = 't_event'
    AND index_name = 'idx_event_sport_active_open'
);
SET @event_active_index_sql = IF(
  @event_active_index_exists = 0,
  'ALTER TABLE t_event ADD INDEX idx_event_sport_active_open (sportid,isactive,open_date,eventid)',
  'SELECT 1'
);
PREPARE event_active_index_statement FROM @event_active_index_sql;
EXECUTE event_active_index_statement;
DEALLOCATE PREPARE event_active_index_statement;

SET @market_event_index_exists = (
  SELECT COUNT(*) FROM information_schema.statistics
  WHERE table_schema = DATABASE()
    AND table_name = 't_market'
    AND index_name = 'idx_market_event_active_sport'
);
SET @market_event_index_sql = IF(
  @market_event_index_exists = 0,
  'ALTER TABLE t_market ADD INDEX idx_market_event_active_sport (eventid,isactive,sportid)',
  'SELECT 1'
);
PREPARE market_event_index_statement FROM @market_event_index_sql;
EXECUTE market_event_index_statement;
DEALLOCATE PREPARE market_event_index_statement;

SET @market_sport_index_exists = (
  SELECT COUNT(*) FROM information_schema.statistics
  WHERE table_schema = DATABASE()
    AND table_name = 't_market'
    AND index_name = 'idx_market_sport_active_event'
);
SET @market_sport_index_sql = IF(
  @market_sport_index_exists = 0,
  'ALTER TABLE t_market ADD INDEX idx_market_sport_active_event (sportid,isactive,eventid)',
  'SELECT 1'
);
PREPARE market_sport_index_statement FROM @market_sport_index_sql;
EXECUTE market_sport_index_statement;
DEALLOCATE PREPARE market_sport_index_statement;

SET @result_market_index_exists = (
  SELECT COUNT(*) FROM information_schema.statistics
  WHERE table_schema = DATABASE()
    AND table_name = 't_matchresult'
    AND index_name = 'idx_matchresult_market'
);
SET @result_market_index_sql = IF(
  @result_market_index_exists = 0,
  'ALTER TABLE t_matchresult ADD INDEX idx_matchresult_market (marketid)',
  'SELECT 1'
);
PREPARE result_market_index_statement FROM @result_market_index_sql;
EXECUTE result_market_index_statement;
DEALLOCATE PREPARE result_market_index_statement;
