# 7. PII, access control, and security

Recommended: keep KG PII-light.
- Only stable keys + approved analytical attributes (country, amounts, dates)
- Do not include names, addresses, phone numbers, email, etc.

Controls:
- separate graphs/databases for sensitive vs non-sensitive (if needed)
- RBAC by label or database (depending on Neo4j edition)

TODO: Add a short PII decision log.
