-- 1
SELECT c_name, c_address, c_acctbal 
FROM customer 
WHERE c_name='Customer#000001024'
Customer#000001024|9wLrRS78uOPy7CHW|-425.09
---> 20

-- 2
SELECT l_orderkey, l_suppkey, l_partkey
FROM lineitem
WHERE l_orderkey<l_suppkey AND l_orderkey<l_partkey AND l_partkey<l_suppkey
 1|48|22
68|97|71
---> 20

-- 3
SELECT DISTINCT l_suppkey, l_shipdate
FROM lineitem
WHERE l_shipdate > '1995-10-10' AND l_shipdate < '1995-10-12'
96|1995-10-11
47|1995-10-11
91|1995-10-11
35|1995-10-11
72|1995-10-11
65|1995-10-11
93|1995-10-11
43|1995-10-11
44|1995-10-11
36|1995-10-11
17|1995-10-11
68|1995-10-11
41|1995-10-11
63|1995-10-11
10|1995-10-11
99|1995-10-11
97|1995-10-11
88|1995-10-11
20|1995-10-11
90|1995-10-11
 1|1995-10-11
 3|1995-10-11
78|1995-10-11
82|1995-10-11
80|1995-10-11
---> 20

-- 4
SELECT SUM(l_extendedprice * l_discount * (1.0-l_tax))
FROM lineitem
WHERE l_discount < 0.07 AND l_quantity < 12
2033945
---> 20

-- 5
SELECT SUM(l_extendedprice * l_discount * (1.0-l_tax)), l_suppkey
FROM lineitem
WHERE l_discount < 0.07 AND l_quantity < 12
GROUP BY l_suppkey
16780.152973|1
16350.955728|2
22548.407697|3
18188.373995|4
18341.569522|5
22537.126185|6
16203.433369|7
18165.546612|8
18729.986662|9
25035.324667|10
21544.958781|11
24433.254052|12
14418.280289|13
26087.491954|14
22490.375821|15
20368.825658|16
19263.932832|17
23971.664148|18
23738.220248|19
21931.103966|20
20735.182293|21
17782.419723|22
20069.967001|23
22039.765861|24
21037.085214|25
20948.905571|26
19338.065276|27
22900.270949|28
21582.416218|29
13866.396121|30
26098.706525|31
21785.044969|32
18001.896743|33
19666.736345|34
19128.550089|35
21990.198365|36
20735.192252|37
26219.356738|38
19731.525871|39
22668.639831|40
19370.206883|41
19034.789487|42
22176.606538|43
18561.616408|44
15606.043033|45
17276.510024|46
18564.679741|47
17398.281607|48
15750.834285|49
 21671.91196|50
23352.361617|51
15074.480291|52
15767.657905|53
16485.877339|54
19665.733507|55
25475.098944|56
16149.038284|57
15459.758207|58
24217.790067|59
24737.938475|60
20447.988866|61
22890.712169|62
22393.042254|63
19985.450311|64
 21357.92574|65
15529.590016|66
19960.179642|67
16337.355229|68
17526.043598|69
15504.315166|70
 24487.49884|71
20646.528309|72
24495.246978|73
20087.118851|74
25760.894296|75
18908.551578|76
18880.456861|77
  21033.1044|78
22916.409429|79
16080.341835|80
 23453.66281|81
18830.702729|82
20872.565876|83
22798.606269|84
23643.148734|85
19484.088275|86
16545.631415|87
15630.541043|88
18459.028668|89
22563.916296|90
26264.144639|91
25004.052243|92
19629.600553|93
13613.491096|94
21923.991581|95
20511.176469|96
20842.028062|97
27607.411079|98
20222.063631|99
21564.041483|100
---> 20

-- 6
SELECT r_name, n_name
FROM region, nation
WHERE r_regionkey < n_nationkey
     AFRICA|ARGENTINA
     AFRICA|BRAZIL
     AFRICA|CANADA
     AFRICA|EGYPT
     AFRICA|ETHIOPIA
     AFRICA|FRANCE
     AFRICA|GERMANY
     AFRICA|INDIA
     AFRICA|INDONESIA
     AFRICA|IRAN
     AFRICA|IRAQ
     AFRICA|JAPAN
     AFRICA|JORDAN
     AFRICA|KENYA
     AFRICA|MOROCCO
     AFRICA|MOZAMBIQUE
     AFRICA|PERU
     AFRICA|CHINA
     AFRICA|ROMANIA
     AFRICA|SAUDI ARABIA
     AFRICA|VIETNAM
     AFRICA|RUSSIA
     AFRICA|UNITED KINGDOM
     AFRICA|UNITED STATES
    AMERICA|BRAZIL
    AMERICA|CANADA
    AMERICA|EGYPT
    AMERICA|ETHIOPIA
    AMERICA|FRANCE
    AMERICA|GERMANY
    AMERICA|INDIA
    AMERICA|INDONESIA
    AMERICA|IRAN
    AMERICA|IRAQ
    AMERICA|JAPAN
    AMERICA|JORDAN
    AMERICA|KENYA
    AMERICA|MOROCCO
    AMERICA|MOZAMBIQUE
    AMERICA|PERU
    AMERICA|CHINA
    AMERICA|ROMANIA
    AMERICA|SAUDI ARABIA
    AMERICA|VIETNAM
    AMERICA|RUSSIA
    AMERICA|UNITED KINGDOM
    AMERICA|UNITED STATES
       ASIA|CANADA
       ASIA|EGYPT
       ASIA|ETHIOPIA
       ASIA|FRANCE
       ASIA|GERMANY
       ASIA|INDIA
       ASIA|INDONESIA
       ASIA|IRAN
       ASIA|IRAQ
       ASIA|JAPAN
       ASIA|JORDAN
       ASIA|KENYA
       ASIA|MOROCCO
       ASIA|MOZAMBIQUE
       ASIA|PERU
       ASIA|CHINA
       ASIA|ROMANIA
       ASIA|SAUDI ARABIA
       ASIA|VIETNAM
       ASIA|RUSSIA
       ASIA|UNITED KINGDOM
       ASIA|UNITED STATES
     EUROPE|EGYPT
     EUROPE|ETHIOPIA
     EUROPE|FRANCE
     EUROPE|GERMANY
     EUROPE|INDIA
     EUROPE|INDONESIA
     EUROPE|IRAN
     EUROPE|IRAQ
     EUROPE|JAPAN
     EUROPE|JORDAN
     EUROPE|KENYA
     EUROPE|MOROCCO
     EUROPE|MOZAMBIQUE
     EUROPE|PERU
     EUROPE|CHINA
     EUROPE|ROMANIA
     EUROPE|SAUDI ARABIA
     EUROPE|VIETNAM
     EUROPE|RUSSIA
     EUROPE|UNITED KINGDOM
     EUROPE|UNITED STATES
MIDDLE EAST|ETHIOPIA
MIDDLE EAST|FRANCE
MIDDLE EAST|GERMANY
MIDDLE EAST|INDIA
MIDDLE EAST|INDONESIA
MIDDLE EAST|IRAN
MIDDLE EAST|IRAQ
MIDDLE EAST|JAPAN
MIDDLE EAST|JORDAN
MIDDLE EAST|KENYA
MIDDLE EAST|MOROCCO
MIDDLE EAST|MOZAMBIQUE
MIDDLE EAST|PERU
MIDDLE EAST|CHINA
MIDDLE EAST|ROMANIA
MIDDLE EAST|SAUDI ARABIA
MIDDLE EAST|VIETNAM
MIDDLE EAST|RUSSIA
MIDDLE EAST|UNITED KINGDOM
MIDDLE EAST|UNITED STATES
---> 25

-- 7
SELECT SUM(c_acctbal), c_name
FROM customer, orders
WHERE c_custkey = o_custkey AND c_acctbal > 9900.0
GROUP BY c_name
217894.16|Customer#000000043
 119557.8|Customer#000000140
 139546.4|Customer#000000200
109753.82|Customer#000001106
---> 25

-- 8
SELECT l_receiptdate
FROM lineitem, orders, customer
WHERE	l_orderkey = o_orderkey AND o_custkey = c_custkey AND	c_name = 'Customer#000000116' AND
	o_orderdate>'1995-03-10' AND o_orderdate<'1995-12-31'
1995-08-18
---> 25

-- 9
SELECT DISTINCT c_name 
FROM lineitem, orders, customer, nation, region
WHERE l_orderkey = o_orderkey AND o_custkey = c_custkey AND 
	c_nationkey = n_nationkey AND n_regionkey = r_regionkey AND r_name = 'EUROPE' AND o_orderstatus = 'O' AND
	o_orderpriority = '1-URGENT' AND l_shipdate > '1995-10-10' AND l_shipdate < '1995-10-15'
Customer#000000046
Customer#000001186
---> 25

-- 10
-- create index on customer.c_custkey
---> 50

-- 11
SELECT SUM(l_discount) 
FROM customer, orders, lineitem
WHERE c_custkey = o_custkey AND o_orderkey = l_orderkey AND c_custkey = 221
1.72
---> 10

-- 12
SELECT SUM(l_discount) 
FROM customer, orders, lineitem
WHERE c_custkey = o_custkey AND o_orderkey = l_orderkey AND c_custkey < 221
-- 448
---> 10

-- 13
SELECT SUM(l_discount) 
FROM customer, orders, lineitem
WHERE c_custkey = o_custkey AND o_orderkey = l_orderkey AND c_custkey > 221 AND c_custkey < 521
-- 591
---> 10

-- 14
SELECT SUM(l_discount) 
FROM customer, orders, lineitem
WHERE c_custkey = o_custkey AND o_orderkey = l_orderkey AND c_custkey > 221 AND c_custkey < 521 AND c_mktsegment = 'HOUSEHOLD'
-- 140
---> 20
