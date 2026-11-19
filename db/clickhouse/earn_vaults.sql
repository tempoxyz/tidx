CREATE VIEW IF NOT EXISTS earn_vaults
DEFINER = CURRENT_USER SQL SECURITY DEFINER AS
SELECT vault, share_token, asset, deployed_block, deployed_at
FROM earn_verified_vaults
WHERE valid_until > now()
