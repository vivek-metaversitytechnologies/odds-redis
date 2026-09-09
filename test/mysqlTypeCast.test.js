const test = require("node:test");
const assert = require("node:assert/strict");
const cast = require("../src/config/mysqlTypeCast");
const Event = require("../src/models/Event");
const Market = require("../src/models/Market");

test("BIT(1) flags normalize to numbers for all DB consumers", () => {
  for (const bit of [0, 1]) {
    let reads = 0;
    const value = cast({ type: "BIT", length: 1, buffer() { reads++; return Buffer.from([bit]); } }, () => assert.fail("Unexpected fallback"));
    assert.equal(value, bit);
    assert.equal(reads, 1);
    assert.equal(Boolean(value), bit === 1);
    assert.equal(Number(value) === 1, bit === 1);
    assert.equal(Event.fromRow({ in_play: value, isactive: value }).inPlay, bit);
    assert.equal(Market.fromRow({ inplay: value }).inPlay, bit);
  }
});

test("NULL BIT flags remain null", () => {
  assert.equal(cast({ type: "BIT", length: 1, buffer: () => null }, () => assert.fail()), null);
});

test("larger BIT fields and other types use native mysql2 conversion", () => {
  for (const [type, length] of [["BIT", 8], ["BIT", 16], ["BLOB", 1], ["TINY", 1], ["VAR_STRING", 1]]) {
    const expected = Buffer.from([0]);
    assert.equal(cast({ type, length, buffer: () => assert.fail("Must not consume field") }, () => expected), expected);
  }
});
