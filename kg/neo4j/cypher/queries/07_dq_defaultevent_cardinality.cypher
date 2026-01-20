MATCH (e:DefaultEvent)
WITH e, size( (e)-[:forContract]->() ) AS n
RETURN
  sum(CASE WHEN n=0 THEN 1 ELSE 0 END) AS missing,
  sum(CASE WHEN n=1 THEN 1 ELSE 0 END) AS ok,
  sum(CASE WHEN n>1 THEN 1 ELSE 0 END) AS too_many;
