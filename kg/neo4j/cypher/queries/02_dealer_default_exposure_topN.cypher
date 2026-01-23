// Input: $topN (default 20)
MATCH (d:Dealer)<-[:hasDealer]-(k:Contract)<-[:forContract]-(e:DefaultEvent)
WITH d, sum(coalesce(e.defaultAmount, 0.0)) AS default_exposure, count(e) AS events
RETURN d.dealerKey AS dealer, default_exposure, events
ORDER BY default_exposure DESC
LIMIT coalesce($topN, 20);

