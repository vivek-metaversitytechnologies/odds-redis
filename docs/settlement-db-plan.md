# Settlement database follow-up

## Evidence from production, 12 September 2026

Read-only inspection found:

| Table | Engine | Relevant indexes | Finding |
| --- | --- | --- | --- |
| `t_matchresult` | MyISAM | Primary key on `id` only | `marketid` lookup uses a full scan; EXPLAIN estimates 10,546 rows |
| `t_fancyresult` | MyISAM | Primary key and `fancyid(191)` | The recently installed prefix index is present |
| `t_matchabondendtie` | MyISAM | Primary key on `id` only | `marketid` lookup uses a full scan; 95 rows at inspection |
| `t_selectionid` | InnoDB | Unique `(selectionid,marketid)` and a `marketid` index | Existing indexes cover runner identification; no new index proposed |

Aggregate duplicate checks found zero duplicate groups for `(marketid,selectionid)`
in `t_matchresult`, `fancyid` in `t_fancyresult`, and `marketid` in
`t_matchabondendtie`. This is a point-in-time observation, not a uniqueness guarantee.

## Ready changes

Socket game-over cleanup now reads market IDs in batches, observes terminal
evidence once per handler call, and retries only the database transaction.
Updates use primary keys in batches of at most 100. Confirmed events select
only active additional markets; historical inactive rows are not swept again.
Redis result queueing occurs before a transaction starts. Redis snapshot removal
and provider unsubscription happen after the connection is released, including
for committed batches when a later batch fails. Fancy settlement status is preserved.

Migration `004_regular_result_lookup_indexes.sql` adds non-unique indexes:

- `t_matchresult(marketid(191),selectionid)`
- `t_matchabondendtie(marketid(191))`, when that table exists

It skips suitable existing leading indexes, preserves duplicate history, and
does not change storage engines. The prefixes fit MyISAM's 1,000-byte key limit
for these utf8mb4 columns.

Apply only this migration during a maintenance window with ingestion paused;
other applications using the result tables may also be blocked by MyISAM DDL.
Do not run the entire migration directory without checking older pending files:
`000`/`001` include runner deduplication, and `002` contains full-column index
definitions that exceed this production MyISAM key limit. They are not prerequisites
for the two targeted indexes in `004`.

If `003` was already executed manually, the corrected version recognizes the
existing prefix index. Do not change checksums of migrations already recorded
by a migration runner; inspect its ledger before any automated rollout.

## Separate InnoDB conversion plan — not applied

Converting just the engine removes MyISAM table locks and lets settlement inserts
participate in rollback. It does not make the current `INSERT ... WHERE NOT EXISTS`
pattern a concurrency-safe uniqueness guarantee. Multiple application writers
must be included in this review.

1. Inventory every writer of the three result tables. Verify that result callbacks,
   settlement workers, and external applications use a consistent transaction
   and market-lock order. Serialize settlement by locking the corresponding market
   row before checking/inserting results, or enforce reviewed unique natural keys.
   Audit exceptional results and regular results together so they cannot both
   settle the same market concurrently.
2. Repeat the duplicate audit and check null/empty IDs. Confirm whether multiple
   winners, corrections, or rollback history require more than one row per key.
   Do not delete or deduplicate result records automatically.
3. Restore a production backup into an isolated MySQL 8 staging database, including
   the real schemas, collations, and data sizes. Test the conversion below there.
   Verify row counts, settlement values, defaults, indexes, and application reads.
4. Test failure after result insertion: both the result insert and market update
   must roll back. Test two concurrent attempts for the same result and overlapping
   discovery, event retirement, and settlement. Verify no duplicate settlements.
5. Measure conversion time and disk use. Schedule downtime for all writers, take
   and verify a restorable backup while writes are quiesced, and repeat the audit.
   Do not rely on `--single-transaction` alone for a consistent MyISAM backup.
6. Apply the reviewed conversion one table at a time, then verify all three engines
   and row counts before resuming writers. MySQL DDL commits independently; if a
   step fails, keep writers paused and resolve or restore from the backup. Do not
   assume that `ROLLBACK` reverses an engine conversion.

Candidate conversion SQL for staging, after those prerequisites:

```sql
ALTER TABLE newkhelo.t_matchresult ENGINE=InnoDB;
ALTER TABLE newkhelo.t_fancyresult ENGINE=InnoDB;
ALTER TABLE newkhelo.t_matchabondendtie ENGINE=InnoDB;
```

After rollout, compare discovery/event-sync durations, settlement failures,
deadlock timestamps, current lock waits, and the failed-tick counter over multiple
busy event cycles. Connectivity health alone does not establish absence of contention.
