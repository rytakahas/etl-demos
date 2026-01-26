-- PLACEHOLDER: Export KGD tables to OneLake files for downstream Python job.
-- Common approach:
-- 1) Keep KGD as tables/views in Warehouse
-- 2) Use Fabric Data Factory "Copy data" activity to export to OneLake:
--    onelake://<workspace>/<lakehouse>/Files/kgd/nodes/*.parquet
--    onelake://<workspace>/<lakehouse>/Files/kgd/edges/*.parquet

-- This file documents the contract; actual export is usually done in pipeline activities.
