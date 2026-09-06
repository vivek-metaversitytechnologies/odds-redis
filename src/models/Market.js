class Market {
  constructor(data = {}) {
    Object.assign(this, data);
  }

  static fromRow(row) {
    const bitNumber = (value) => (Buffer.isBuffer(value) ? Number(value[0] || 0) : Number(value || 0));
    return new Market({
      ...row,
      updatedOn: row.updatedon,
      betDelay: row.betdelay,
      inplay: bitNumber(row.inplay),
      inPlay: bitNumber(row.inplay),
      minBetRate: row.minbetrate ?? 0,
      maxBetRate: row.maxbetrate ?? 0,
      isRedisUpdated: row.is_redis_updated,
      displayMessage: row.display_message ?? row.remarks ?? null,
      isSuspended: row.issuspended,
      isRolledBack: row.is_rolled_back,
      maximumProfit: row.maximumprofit ?? 0,
    });
  }
}

module.exports = Market;
