-- Point lookup: block by number.
\set b random(:min_block, :max_block)
SELECT * FROM blocks WHERE num = :b;
