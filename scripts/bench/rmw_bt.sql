\set k random(1,1000000)
\set v random(1,1000000)
UPDATE eval_bt
SET v = :v
WHERE ctid = (SELECT ctid FROM eval_bt WHERE k = :k LIMIT 1);
