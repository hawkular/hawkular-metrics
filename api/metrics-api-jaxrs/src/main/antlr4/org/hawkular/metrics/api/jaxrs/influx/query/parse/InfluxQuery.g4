/*
 * Copyright 2014-2015 Red Hat, Inc. and/or its affiliates
 * and other contributors as indicated by the @author tags.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
grammar InfluxQuery;

query: listSeries EOF
     | selectQuery EOF
     ;

listSeries: LIST SERIES;

selectQuery: SELECT selectColumns fromClause (groupByClause? whereClause | whereClause? groupByClause)? limitClause?
           orderClause?
           ;

selectColumns: STAR #starColumn
             | columnDefinition (',' columnDefinition)* #columnDefinitionList
             ;

columnDefinition: rawColumnDefinition alias?
                | aggregatedColumnDefinition alias?
                ;

rawColumnDefinition: prefix? name;

aggregatedColumnDefinition: functionCall;

fromClause: FROM name alias?;

groupByClause: GROUP BY ID '(' TIMESPAN ')';

whereClause: WHERE booleanExpression;

booleanExpression: operand '=' operand #eqExpression
                 | operand '>' operand #gtExpression
                 | operand '<' operand #ltExpression
                 | operand '<>' operand #neqExpression
                 | '(' booleanExpression ')' #parenthesisExpression
                 | booleanExpression AND booleanExpression #andExpression
                 | booleanExpression OR booleanExpression #orExpression
                 ;

operand: prefix? name #nameOperand
       | TIMESPAN #absoluteMomentOperand
       | ID '(' ')' DASH TIMESPAN #pastMomentOperand
       | ID '(' ')' PLUS TIMESPAN #futureMomentOperand
       | ID '(' ')' #presentMomentOperand
       | DATE_STRING #dateOperand
       | DASH? INT #integerOperand
       | DASH? FLOAT #doubleOperand
       ;

limitClause: LIMIT INT;

orderClause: ORDER 'asc' #orderAsc
           | ORDER 'desc' #orderDesc
           ;

name: ID # idName
    | DOUBLE_QUOTED_STRING # stringName
    ;

alias: AS ID;

prefix: ID '.';

functionCall: ID '(' functionArgumentList? ')';
functionArgumentList: functionArgument (',' functionArgument)*;
functionArgument: prefix? name #nameFunctionArgument
                | SINGLE_QUOTED_STRING #stringFunctionArgument
                | DASH? FLOAT #doubleFunctionArgument
                | DASH? INT #integerFunctionArgument;

LIST: L I S T;
SERIES: S E R I E S;
SELECT: S E L E C T;
FROM: F R O M;
AS: A S;
WHERE: W H E R E;
AND: A N D;
OR: O R;
GROUP: G R O U P;
BY: B Y;
LIMIT: L I M I T;
ORDER: O R D E R;

TIMESPAN: INT TIMEUNIT;
fragment TIMEUNIT: U | S | M | H | D | W;

ID: ID_LETTER (ID_LETTER | DIGIT)*;
fragment ID_LETTER: [a-zA-Z] | '_';

STAR: '*';
DASH: '-';
PLUS: '+';

DATE_STRING: '\'' DIGIT DIGIT DIGIT DIGIT DASH DIGIT DIGIT DASH DIGIT DIGIT (' ' DIGIT DIGIT ':' DIGIT DIGIT ':' DIGIT DIGIT ('.' DIGIT DIGIT DIGIT)?)? '\'';

SINGLE_QUOTED_STRING: '\'' (SINGLE_QUOTED_STRING_ESC|.)*? '\'';
fragment SINGLE_QUOTED_STRING_ESC: '\\\'' | '\\\\';

DOUBLE_QUOTED_STRING: '"' (DOUBLE_QUOTED_STRING_ESC|.)*? '"';
fragment DOUBLE_QUOTED_STRING_ESC: '\\"' | '\\\\';

INT: DIGIT+;
FLOAT: DIGIT+ '.' DIGIT*
     | '.' DIGIT+
     ;

WITHESPACE: [ \t\n\r]+ -> skip;

fragment DIGIT: [0-9];

fragment A: [aA];
fragment B: [bB];
fragment C: [cC];
fragment D: [dD];
fragment E: [eE];
fragment F: [fF];
fragment G: [gG];
fragment H: [hH];
fragment I: [iI];
fragment J: [jJ];
fragment K: [kK];
fragment L: [lL];
fragment M: [mM];
fragment N: [nN];
fragment O: [oO];
fragment P: [pP];
fragment Q: [qQ];
fragment R: [rR];
fragment S: [sS];
fragment T: [tT];
fragment U: [uU];
fragment V: [vV];
fragment W: [wW];
fragment X: [xX];
fragment Y: [yY];
fragment Z: [zZ];
