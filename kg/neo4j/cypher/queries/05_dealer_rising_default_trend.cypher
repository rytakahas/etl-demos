WITH date() AS today
MATCH (d:Dealer)<-[:hasDealer]-(k:Contract)<-[:forContract]-(e:DefaultEvent)
WITH d, date(e.eventDate) AS dt
WITH d,
  sum(CASE WHEN dt >= (today - duration('P90D')) THEN 1 ELSE 0 END) AS last_3m,
  sum(CASE WHEN dt >= (today - duration('P180D')) AND dt < (today - duration('P90D')) THEN 1 ELSE 0 END) AS prev_3m
RETURN d.dealerKey AS dealer, last_3m, prev_3m, (last_3m - prev_3m) AS delta
ORDER BY delta DESC
LIMIT 50;
