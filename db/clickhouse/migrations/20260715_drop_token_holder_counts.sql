-- token_holder_counts declares a refresh dependency on
-- token_balances_snapshot, so remove the dependent before replacing its
-- source. Catalog reconciliation recreates it later in dependency order.
DROP VIEW IF EXISTS token_holder_counts SYNC
