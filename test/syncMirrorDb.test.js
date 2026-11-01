const test = require("node:test");
const assert = require("node:assert/strict");
const { columnDefinitions, addableDefinition, insertSql } = require("../scripts/sync-mirror-db");

test("column definitions are read from SHOW CREATE TABLE", () => {
  const ddl = [
    "CREATE TABLE `t_event` (",
    "  `id` int NOT NULL AUTO_INCREMENT,",
    "  `tv` varchar(512) DEFAULT NULL,",
    "  `appid` int NOT NULL",
    "  PRIMARY KEY (`id`),",
    "  UNIQUE KEY `uq_t_event_eventid` (`eventid`)",
    ") ENGINE=MyISAM",
  ].join("\n");
  assert.deepEqual([...columnDefinitions(ddl)], [
    ["id", "int NOT NULL AUTO_INCREMENT"],
    ["tv", "varchar(512) DEFAULT NULL"],
    ["appid", "int NOT NULL"],
  ]);
});

test("an added column never breaks the other application's inserts", () => {
  assert.equal(addableDefinition("int NOT NULL"), "int NULL");
  assert.equal(addableDefinition("int NOT NULL DEFAULT '0'"), "int NOT NULL DEFAULT '0'");
  assert.equal(addableDefinition("varchar(512) DEFAULT NULL"), "varchar(512) DEFAULT NULL");
});

test("copied rows are inserted by column name, and upserted where asked", () => {
  const [sql, [rows]] = insertSql("t_event", ["eventid", "tv"], [{ id: 9, eventid: 1, tv: null }], { upsert: true });
  assert.equal(sql, "INSERT INTO `t_event` (`eventid`,`tv`) VALUES ? ON DUPLICATE KEY UPDATE `eventid`=VALUES(`eventid`),`tv`=VALUES(`tv`)");
  assert.deepEqual(rows, [[1, null]]);
});
