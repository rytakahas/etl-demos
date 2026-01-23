MATCH (e:DefaultEvent)
WHERE NOT (e)-[:forContract]->(:Contract)
RETURN count(e) AS default_events_missing_contract;

