/*
 * Copyright 2014-2017 Red Hat, Inc. and/or its affiliates
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
grammar TagQuery;

@header {
/*
 * Copyright 2014-2017 Red Hat, Inc. and/or its affiliates
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
}


//lexer
tagquery
  : object
  ;

object
  : pair
  | '(' object ')'
  | object logical_operator object
  ;

pair
  : key
  | existence_operator key
  | key boolean_operator value
  | key regex_operator value
  | key array_operator array
  ;

logical_operator
  : AND
  | OR
  ;

boolean_operator
  : EQUAL
  | NOTEQUAL
  ;

regex_operator
  : REGEXMATCH
  | NOTREGEXMATCH
  ;

array_operator
  : IN
  | NOT IN
  ;

existence_operator
  : NOT
  ;

array
  : '[' value (',' value)* ']'
  | '[' ']' // empty array
  ;

value
  : SIMPLETEXT
  | COMPLEXTEXT
  ;

key
  : SIMPLETEXT
  ;


//parser
OR: O R;
AND: A N D;
NOT: N O T;
EQUAL: '=';
NOTEQUAL: '!=';
REGEXMATCH: '~';
NOTREGEXMATCH: '!~';
IN: I N;

SIMPLETEXT  : [a-zA-Z_0-9.]+ ;
COMPLEXTEXT :  '\'' (ESC | ~[\'\\])* '\'' ;

WS  :   [ \t\n\r]+ -> skip ;


//fragments
fragment ESC : '\\' ([\'\\/bfnrt] | UNICODE) ;
fragment UNICODE : 'u' HEX HEX HEX HEX ;
fragment HEX : [0-9a-fA-F] ;

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