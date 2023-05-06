SELECT DISTINCT l_suppkey, l_shipdate
FROM lineitem
WHERE l_shipdate > '1995-10-10' AND l_shipdate < '1995-10-12'