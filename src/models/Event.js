class Event {
  constructor(data = {}) {
    Object.assign(this, {
      id: data.id ?? null,
      seriesid: data.seriesid ?? null,
      sportid: data.sportid ?? null,
      eventid: data.eventid ?? null,
      eventname: data.eventname ?? null,
      openDate: data.openDate ?? null,
      status: data.status ?? null,
      isActive: data.isActive ?? null,
      createdon: data.createdon ?? null,
      updatedOn: data.updatedOn ?? null,
      fancypause: data.fancypause ?? null,
      betLock: data.betLock ?? null,
      isRedisUpdated: data.isRedisUpdated ?? null,
      inPlay: data.inPlay ?? null,
      fancyLock: data.fancyLock ?? null,
      bookmaker: data.bookmaker ?? false,
      fancy: data.fancy ?? false,
      channelId: data.channelId ?? null,
    });
  }

  static fromRow(row) {
    return new Event({ ...row, openDate: row.open_date, isActive: row.isactive,
      updatedOn: row.updatedon, betLock: row.betlock,
      isRedisUpdated: row.is_redis_updated, inPlay: row.in_play,
      fancyLock: row.fancylock, channelId: row.channel_id });
  }
}

module.exports = Event;
