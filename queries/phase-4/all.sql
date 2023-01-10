SELECT SUM(l_extendedprice * l_discount * (1.0-l_tax)), l_suppkey
FROM lineitem
WHERE l_discount < 0.07 AND l_quantity < 12.0
GROUP BY l_suppkey

SELECT SUM(ps_supplycost), ps_suppkey
FROM partsupp 
GROUP BY ps_suppkey

SELECT SUM(c_acctbal), c_name 
FROM customer
GROUP BY c_name

SELECT DISTINCT o_orderdate
FROM orders
WHERE	o_orderdate>'1995-03-10' AND o_orderdate<'1995-03-20'

SELECT DISTINCT c_address
FROM customer
WHERE c_nationkey = 6

SELECT SUM(l_discount) 
FROM lineitem
WHERE l_orderkey>100 AND l_orderkey<400

SELECT DISTINCT c_nationkey
FROM customer
WHERE c_custkey > 100 AND c_custkey < 150

SELECT DISTINCT l_suppkey, l_shipdate, l_commitdate, l_receiptdate
FROM lineitem
WHERE l_shipdate > '1995-10-10' AND l_shipdate < '1995-10-15'

SELECT DISTINCT l_discount 
FROM lineitem
WHERE l_returnflag = 'N' AND l_linestatus = 'F'

SELECT SUM(l_extendedprice * l_discount * (1.0-l_tax))
FROM lineitem
WHERE l_discount < 0.07 AND l_quantity < 12.0

