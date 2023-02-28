SELECT l_orderkey 
FROM lineitem
WHERE l_quantity > 30

SELECT SelectAtts
FROM Tables
WHERE AndList

l_orderkey
SelectAtts
Atts
YY_NAME
attsToSelect = {l_orderkey}

lineitem
Tables
YY_NAME
tables = {lineitem}

l_quantity  >           30
AndList
Condition
Literal     BoolComp    Literal
predicates = {ComparisonOp{left, code, right}}

l_quantity
Literal
YY_NAME
left = {code = NAME, value = l_quantity}

>
BoolComp
>
code = GREATER_THAN

30
Literal
YY_INTEGER
right = {code = INTEGER, value = 30}