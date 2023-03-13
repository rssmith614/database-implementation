SELECT SUM(ps_supplycost), s_suppkey 
FROM part, supplier, partsupp 
WHERE p_partkey = ps_partkey AND s_suppkey = ps_suppkey AND s_acctbal > 2500.00
GROUP BY s_suppkey

SELECT SelectAtts
FROM Tables
WHERE AndList
GROUP BY Atts

SUM ( ps_supplycost ) , suppkey
SelectAtts
Function            ',' Atts
SUM '(' CompoundExp ')'
attsToSelect = {}

ps_supplycost
CompoundExp
SimpleExp
YY_NAME

suppkey
Atts
YY_NAME
attsToSelect = {suppkey}

part, supplier, partsupp
Tables
Tables      ',' YY_NAME
tables = {partsupp}

part    , supplier
Tables ',' YY_NAME
tables = {supplier, partsupp}

part
YY_NAME
tables = {part, supplier, partsupp}

p_partkey = ps_partkey  AND s_suppkey = ps_suppkey AND s_acctbal > 2500.00
AndList
Condition               AND AndList

p_partkey   =           ps_partkey
Condition
Literal     BoolComp    Literal
predicate = {ComparisonOp {left, code, right}}

p_partkey
Literal
YY_NAME
left = {code = NAME, value = p_partkey}

=
BoolComp
=
code = EQUALS

ps_partkey
Literal
YY_NAME
right = {code = NAME, value = ps_partkey}

s_suppkey = ps_suppkey  AND s_acctbal > 2500.00
Condition               AND AndList

s_suppkey   =           ps_suppkey
Condition
Literal     BoolComp    Literal
predicate = {ComparisonOp{left, code, right}, ComparisonOp{left, code, right}}

s_suppkey
Literal
YY_NAME
left = {code = NAME, value = s_suppkey}

=
BoolComp
=
code = EQUALS

ps_suppkey
Literal
YY_NAME
right = {code = NAME, value = ps_suppkey}

s_acctbal   >           2500.00
AndList
Comparison
Literal     BoolComp    Literal
predicate = {ComparisonOp{left, code, right}, ComparisonOp{left, code, right}, ComparisonOp{left, code, right}}

s_acctbal
Literal
YY_NAME
left = {code = NAME, value = s_acctbal}

>
BoolComp
>
code = GREATER_THAN

2500.00
Literal
YY_FLOAT
right = {code = FLOAT, value = 2500.00}

s_suppkey
Atts
YY_NAME
groupingAtts = {s_suppkey}