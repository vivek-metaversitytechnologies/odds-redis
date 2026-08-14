INSERT INTO t_matchfancy
  (fancyid,name,oddstype,status,maxliabilityper_market,betdelay,minbet,maxbet,eventid,
   issuspendedbyadmin,isactive,mtype,isshow,is_show,suspendedby,remarks,createdon,updatedon,
   matchname,sportid,provider,isbettable,isplay,maxliabilityperbet)
SELECT
  marketid,
  marketname,
  'LINE',
  CASE WHEN isactive = 1 THEN 'OPEN' ELSE 'SUSPENDED' END,
  COALESCE(maxbet, 1),
  COALESCE(betdelay, 5),
  COALESCE(minbet, 100),
  COALESCE(maxbet, 1),
  eventid,
  0,
  isactive,
  'line-market',
  isactive,
  isactive,
  '',
  display_message,
  COALESCE(createdon, NOW()),
  COALESCE(updatedon, NOW()),
  matchname,
  sportid,
  'RS',
  1,
  inplay,
  COALESCE(maxbet, 1)
FROM t_market
WHERE LOWER(marketname) LIKE '%line%'
ON DUPLICATE KEY UPDATE
  name=VALUES(name),
  oddstype=VALUES(oddstype),
  status=VALUES(status),
  betdelay=VALUES(betdelay),
  minbet=VALUES(minbet),
  maxbet=VALUES(maxbet),
  eventid=VALUES(eventid),
  isactive=VALUES(isactive),
  mtype=VALUES(mtype),
  isshow=VALUES(isshow),
  is_show=VALUES(is_show),
  remarks=VALUES(remarks),
  updatedon=VALUES(updatedon),
  matchname=VALUES(matchname),
  sportid=VALUES(sportid),
  isplay=VALUES(isplay);

DELETE FROM t_market WHERE LOWER(marketname) LIKE '%line%';
