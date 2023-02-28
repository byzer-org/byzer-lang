
grammar DSLSQL;

@header {
package streaming.dsl.parser;
}

//load jdbc.`mysql1.tb_v_user` as mysql_tb_user;
//save csv_input_result as json.`/tmp/todd/result_json` partitionBy uid;
statement
    : (sql ender)*
    ;


sql
    : LOAD format '.' path where? expression? booleanExpression* as tableName
    | SAVE (overwrite|append|errorIfExists|ignore)* tableName as format '.' path where? expression? booleanExpression* (PARTITIONBY col? colGroup*)?
    | SELECT ~(';')* as tableName
    | INSERT ~(';')*
    | CREATE ~(';')*
    | DROP ~(';')*
    | REFRESH ~(';')*
    | SET setKey '=' setValue where? expression? booleanExpression*
    | CONNECT format where? expression? booleanExpression* (as db)?
    | (TRAIN|RUN|PREDICT) tableName (as|into) format '.' path where? expression? booleanExpression* asTableName*
    | REGISTER format '.' path as functionName where? expression? booleanExpression*
    | UNREGISTER format '.' path where? expression? booleanExpression*
    | INCLUDE format '.' path where? expression? booleanExpression*
    | EXECUTE_COMMAND (commandValue|rawCommandValue)*
    | SIMPLE_COMMENT
    ;

//keywords
AS: 'as';
INTO: 'into';
LOAD: 'load';
SAVE: 'save';
SELECT: 'select';
INSERT: 'insert';
CREATE: 'create';
DROP: 'drop';
REFRESH: 'refresh';
SET: 'set';
CONNECT: 'connect';
TRAIN: 'train';
RUN: 'run';
PREDICT: 'predict';
REGISTER: 'register';
UNREGISTER: 'unregister';
INCLUDE: 'include';
OPTIONS:'options';
WHERE:'where';
PARTITIONBY:'partitionBy'|'partitionby';
OVERWRITE:'overwrite';
APPEND:'append';
ERRORIfExists:'errorifexists';
IGNORE:'ignore';


as: AS;
into: INTO;

saveMode: (OVERWRITE|APPEND|ERRORIfExists|IGNORE);

where: OPTIONS|WHERE;

whereExpressions: where expression? booleanExpression*;


overwrite
    : OVERWRITE
    ;

append
    : APPEND
    ;

errorIfExists
    : ERRORIfExists
    ;

ignore
    : IGNORE
    ;

booleanExpression
    : 'and' expression
    ;

expression
    : qualifiedName '=' (STRING|BLOCK_STRING)
    ;

ender
    :';'
    ;

format
    : identifier
    ;

path
    : quotedIdentifier | identifier
    ;

commandValue: quotedIdentifier | STRING | BLOCK_STRING;
rawCommandValue: (identifier | '-' | '/' | '>' | '<' | '.' | '~')+ ;


setValue
    : qualifiedName | quotedIdentifier | STRING | BLOCK_STRING
    ;

setKey
    : qualifiedName
    ;

db
    :qualifiedName | identifier
    ;

asTableName
    : 'as' tableName
    ;

tableName
    : identifier
    ;

functionName
    : identifier
    ;

colGroup
    : ',' col
    ;

col
    : identifier
    ;

qualifiedName
    : identifier ('.' identifier)*
    ;

identifier
    : strictIdentifier
    ;

strictIdentifier
    : IDENTIFIER
    | quotedIdentifier
    ;

quotedIdentifier
    : BACKQUOTED_IDENTIFIER
    ;


STRING
    : '\'' ( ~('\''|'\\') | ('\\' .) )* '\''
    | '"' ( ~('"'|'\\') | ('\\' .) )* '"'
    ;

BLOCK_STRING
    : '\'\'\'' ~[+] .*? '\'\'\''
    ;

IDENTIFIER
    : (LETTER | DIGIT | '_')+
    ;

BACKQUOTED_IDENTIFIER
    : '`' ( ~'`' | '``' )* '`'
    ;

fragment DIGIT
    : [0-9]
    ;

fragment LETTER
    : [a-zA-Z]
    ;

EXECUTE_COMMAND
    : EXECUTE_TOKEN (LETTER | DIGIT | '_')+
    ;


EXECUTE_TOKEN: '!';

SIMPLE_COMMENT
    : '--' ~[\r\n]* '\r'? '\n'? -> channel(HIDDEN)
    ;

BRACKETED_EMPTY_COMMENT
    : '/**/' -> channel(HIDDEN)
    ;

BRACKETED_COMMENT
    : '/*' ~[+] .*? '*/' -> channel(HIDDEN)
    ;

WS
    : [ \r\n\t]+ -> channel(HIDDEN)
    ;

// Catch-all for anything we can't recognize.
// We use this to be able to ignore and recover all the text
// when splitting statements with DelimiterLexer
UNRECOGNIZED
    : .
    ;
