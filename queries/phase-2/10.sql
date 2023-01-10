SELECT SUM(l_extendedprice * l_discount), n_name
FROM customer, orders, lineitem, nation
WHERE c_custkey = o_custkey AND o_orderkey = l_orderkey AND c_nationkey = n_nationkey AND
	c_acctbal < 1000.00 AND l_quantity > 30 AND l_discount < 0.03
GROUP BY n_name
