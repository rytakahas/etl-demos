# 1. Scope and competency questions

## 1.1 Scope (what this KG covers)

**Domain:** Banking / auto-loan style demo (Customer–Contract–Dealer–Vehicle–DefaultEvent–Payment).

**Primary use cases**
- Portfolio risk analytics (defaults, recoveries, exposure trends)
- Dealer performance analytics (defaults by dealer, approval volume, profitability proxy)
- Graph-style investigations (paths from Dealer → Contracts → Defaults)
- (Optional) GraphRAG over policy/docs + structured KG

**Out of scope (explicit)**
- KYC / PII-heavy attributes (names, addresses, phone numbers)
- Model training/inference services (unless explicitly added in `deploy/`)

## 1.2 Competency questions (must-answer queries)

These are the **acceptance criteria** for the KG.

### Risk / Portfolio
1) What is the default rate by month, country, and segment?
2) Which dealers have the highest default exposure in the last N months?
3) For a given customer (by key), what contracts and default events exist?

### Dealer performance
4) Dealer ranking by (approvals, defaults, recovery rate), sliced by country and fuel type.
5) Identify dealers with rising default trend over last 3 months.

### Operations / Data quality
6) Are there contracts missing customer links? (should be 0 in conformant KG)
7) Are default events always linked to exactly one contract?

### GraphRAG (optional)
8) Given a policy paragraph, retrieve the most relevant contracts/default events and supporting text chunks.
