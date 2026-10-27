-- The vendor's score/TV iframe HTML for an event. Apply to the mirror database too: mirrored
-- event upserts write this column.
SET @event_tv_exists = (
  SELECT COUNT(*) FROM information_schema.columns
  WHERE table_schema = DATABASE() AND table_name = 't_event' AND column_name = 'tv'
);
SET @event_tv_sql = IF(
  @event_tv_exists = 0,
  'ALTER TABLE t_event ADD COLUMN tv VARCHAR(512) NULL',
  'SELECT 1'
);
PREPARE event_tv_statement FROM @event_tv_sql;
EXECUTE event_tv_statement;
DEALLOCATE PREPARE event_tv_statement;
