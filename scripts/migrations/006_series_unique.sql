-- t_series had no unique key, so competitionSync's ON DUPLICATE KEY UPDATE never matched and
-- every sync inserted each competition again. Keeps the oldest row per competition (with the
-- newest row's name and status, as the upsert would have written), deletes the copies and adds
-- the key. Apply to the mirror database too: mirrored series upserts rely on the same key.
SET @series_index_exists = (
  SELECT COUNT(*)
  FROM information_schema.statistics
  WHERE table_schema = DATABASE()
    AND table_name = 't_series'
    AND index_name = 'uq_series_seriesid_sportid'
);
SET @series_refresh_sql = IF(
  @series_index_exists = 0,
  'UPDATE t_series keeper JOIN (SELECT seriesid, sportid, MIN(id) keeper_id, MAX(id) latest_id FROM t_series GROUP BY seriesid, sportid HAVING COUNT(*) > 1) duplicates ON keeper.id = duplicates.keeper_id JOIN t_series latest ON latest.id = duplicates.latest_id SET keeper.seriesname = latest.seriesname, keeper.isactive = latest.isactive, keeper.status = latest.status, keeper.updatedon = latest.updatedon',
  'SELECT 1'
);
PREPARE series_refresh_statement FROM @series_refresh_sql;
EXECUTE series_refresh_statement;
DEALLOCATE PREPARE series_refresh_statement;
SET @series_cleanup_sql = IF(
  @series_index_exists = 0,
  'DELETE duplicate_row FROM t_series duplicate_row JOIN t_series keeper ON keeper.seriesid = duplicate_row.seriesid AND keeper.sportid = duplicate_row.sportid AND keeper.id < duplicate_row.id',
  'SELECT 1'
);
PREPARE series_cleanup_statement FROM @series_cleanup_sql;
EXECUTE series_cleanup_statement;
DEALLOCATE PREPARE series_cleanup_statement;
SET @series_index_sql = IF(
  @series_index_exists = 0,
  'ALTER TABLE t_series ADD UNIQUE KEY uq_series_seriesid_sportid (seriesid, sportid)',
  'SELECT 1'
);
PREPARE series_index_statement FROM @series_index_sql;
EXECUTE series_index_statement;
DEALLOCATE PREPARE series_index_statement;
