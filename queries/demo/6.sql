SELECT r_name, n_name
FROM region, nation
WHERE r_regionkey < n_nationkey