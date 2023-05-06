SELECT SUM(c_acctbal), c_name
FROM customer, orders
WHERE c_custkey = o_custkey AND c_acctbal > 9900.0
GROUP BY c_name