# 6. Change strategy

## Full rebuild (dev/demo)
- Wipe graph and re-import.

## Incremental upsert (production)
- MERGE nodes by key
- MERGE relationships by endpoints
- SET properties

Deletions:
- Prefer soft delete flags unless policy allows hard delete.
