WITH date() AS today, coalesce($months, 6) AS m
MATCH (d:Dealer)<-[:hasDealer]-(k:Contract)<-[:forContract]-(e:DefaultEvent)
WHERE date(e.eventDate) >= (today - duration('P' + toString(30*m) + 'D'))
RETURN
  d.dealerKey AS dealer,
  sum(coalesce(e.defaultAmount, 0.0)) AS default_exposure,
  count(e) AS default_events
ORDER BY default_exposure DESC
LIMIT coalesce($topN, 20);
