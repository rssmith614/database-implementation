SQL ::= SELECT SelectAtts FROM Tables WHERE AndList |
        SELECT SelectAtts FROM Tables WHERE AndList GROUP BY Atts

Tables ::= YY_NAME | Tables ',' YY_NAME

SelectAtts ::= Function ',' Atts | Function | Atts | DISTINCT Atts 
Atts ::= YY_NAME | Atts ',' YY_NAME 

Function ::= SUM '(' CompoundExp ')'

++++++++++++++++++++++++++++++++++++++++++++++++++++
Function ::= SUM '(' CompoundExp ')' | COUNT '(' CompoundExp ')' | AVERAGE '(' CompoundExp ')'

Function ::= FuncType '(' CompoundExp ')'
FuncType ::= SUM | COUNT | AVERAGE | MIN | MAX 
++++++++++++++++++++++++++++++++++++++++++++++++++++

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


++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
struct FuncOperator* finalFunction = {}; // the aggregate function
struct TableList* tables = {}; // the list of tables in the query
struct AndList* predicate = {}; // the predicate in WHERE
struct NameList* groupingAtts = {}; // grouping attributes
struct NameList* attsToSelect = {}; // the attributes in SELECT
int distinctAtts = 0; // 1 if there is a DISTINCT in a non-aggregate query 


SELECT SUM(l_extendedprice * l_discount * (1.0-l_tax)) FROM lineitem WHERE l_orderkey > -1
SELECT SelectAtts                                      FROM Tables   WHERE AndList
---------------------
groupingAtts = {}

SUM  ( l_extendedprice * l_discount * (1.0-l_tax) )
SelectAtts
Function
SUM '(' CompoundExp ')'
---------------------
attsToSelect = {}

l_extendedprice *       l_discount * (1.0-l_tax)
CompoundExp
SimpleExp       Op      CompoundExp
YY_NAME         '*'

l_discount      *       (1.0-l_tax)
CompoundExp
SimpleExp       Op      CompoundExp
YY_NAME         '*'

(       1.0-l_tax             )
CompoundExp
'('     CompoundExp         ')'

1.0             -               l_tax
CompoundExp
SimpleExp       Op              CompoundExp
YY_FLOAT        '-'             CompoundExp

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
