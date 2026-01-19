# 4. Identity & IRI minting rules

## 4.1 Base namespace
`https://example.org/honda-bank/kg#`

## 4.2 IRI (Internationalized Resource Identifier) patterns (stable IDs)
- Customer: `...#Customer/{customerKey}`
- Dealer: `...#Dealer/{dealerKey}`
- Vehicle: `...#Vehicle/{vehicleKey}`
- Country: `...#Country/{countryKey}`
- Contract: `...#Contract/{contractKey}`
- DefaultEvent: `...#DefaultEvent/{eventId}`

**Rules**
- Keys must be stable across re-runs.
- Avoid random UUID unless source provides stable IDs.
- URL-encode unsafe characters.

## 4.3 Neo4j merge strategy
Enforce uniqueness constraints and load using MERGE on key.
