// Requires e.eventDate as date
WITH date() AS today
MATCH (e:DefaultEvent)-[:forContract]->(k:Contract)
WHERE e.eventDate IS NOT NULL
WITH e, k,
     CASE
       WHEN e.eventDate >= (today - duration('P90D')) THEN 'last_3m'
       WHEN e.eventDate >= (today - duration('P180D')) AND e.eventDate < (today - duration('P90D')) THEN 'prev_3m'
       ELSE NULL
     END AS bucket
WHERE bucket IS NOT NULL
WITH k, bucket, count(e) AS cnt
WITH k,
     sum(CASE WHEN bucket='last_3m' THEN cnt ELSE 0 END) AS last_3m,
     sum(CASE WHEN bucket='prev_3m' THEN cnt ELSE 0 END) AS prev_3m
RETURN k.contractKey AS contract, last_3m, prev_3m, (last_3m - prev_3m) AS delta
ORDER BY delta DESC
LIMIT 50;

