-- SQL statements can have one of two signatures
-- CAPS are terminal statements, others are non-terminals, must be defined recursively
SQL ::= SELECT SelectAtts FROM Tables WHERE AndList |
        SELECT SelectAtts FROM Tables WHERE AndList GROUP BY Atts 

THE LAWS OF QUERY EXECUTION
1. must always have select or join
2. must always have project, sum, or group by
3. distinct can only be present if project is present

-- recursive definition of non-terminal Tables
-- YY_NAME is a terminal
Tables ::= YY_NAME | Tables ',' YY_NAME

SelectAtts ::= Function ',' Atts | Function | Atts | DISTINCT Atts 
Atts ::= YY_NAME | Atts ',' YY_NAME 

Function ::= SUM '(' CompoundExp ')' 
CompoundExp ::= SimpleExp Op CompoundExp | '(' CompoundExp ')' Op CompoundExp |
 		       '(' CompoundExp ')' | SimpleExp | '-' CompoundExp 

Op ::= '-' | '+' | '*' | '/' 
BoolComp ::= '<' | '>' | '=' 

Literal ::= YY_STRING | YY_FLOAT | YY_INTEGER | YY_NAME 
SimpleExp ::= YY_FLOAT | YY_INTEGER | YY_NAME

AndList ::= Condition | Condition AND AndList 
Condition ::= Literal BoolComp Literal 


TableList* tables = {}; // the list of tables in the query
AndList* predicate = {}; // the predicate in WHERE
NameList* attsToSelect = {}; // the attributes in SELECT


SELECT c_address, c_phone, c_acctbal FROM customer WHERE c_name = 'Customer#000000010'
SELECT SelectAtts                    FROM Tables   WHERE AndList

c_address, c_phone, c_acctbal
Atts
SelectAtts
Atts                 ','    YY_NAME
c_address, c_phone    ,     c_acctbal
attsToSelect = {c_acctbal}

c_address, c_phone
Atts
Atts                 ','    YY_NAME
c_address             ,     c_phone
attsToSelect = {c_phone, c_acctbal}

c_address
Atts
YY_NAME
attsToSelect = {c_address, c_phone, c_acctbal}

customer
Tables
YY_NAME
tables = {customer}

c_name = 'Customer#000000010'
AndList
Condition
Literal     BoolComp    Literal
c_name      =           'Customer#000000010'
predicate = {ComparisonOp {left, code, right}}

c_name
Literal
YY_NAME
left = {code = NAME, value = c_name}

=
BoolComp
=
code = EQUALS

'Customer#000000010'
Literal
YY_STRING
right = {code = STRING, value = 'Customer#000000010'}


SELECT SUM(l_extendedprice * l_discount * (1.0-l_tax)) FROM lineitem WHERE l_orderkey > -1
SELECT SelectAtts                                      FROM Tables   WHERE AndList

SUM  ( l_extendedprice * l_discount * (1.0-l_tax) )
SelectAtts
Function
SUM '(' CompoundExp ')'
attsToSelect = {}

l_extendedprice *       l_discount * (1.0-l_tax)
CompoundExp
SimpleExp       Op      CompoundExp
YY_NAME         '*'

l_discount      *       (1.0-l_tax)
CompoundExp
SimpleExp       Op      CompoundExp
YY_NAME         '*'

(1.0-l_tax)
CompoundExp
'(' CompoundExp ')'

1.0             -       l_tax
CompoundExp
SimpleExp       Op      CompoundExp
YY_FLOAT        '-'

l_tax
CompoundExp
SimpleExp
YY_NAME

lineitem
Tables
YY_NAME

l_orderkey      >               -1
AndList
Condition
Literal         BoolComp        Literal
YY_NAME         '>'             YY_INTEGER