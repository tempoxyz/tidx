Fixed archive queries through a dedicated PostgreSQL API login by recreating its ClickHouse FDW user mapping alongside the sync owner's mapping during tiered bootstrap, including hot-loaded chains.
