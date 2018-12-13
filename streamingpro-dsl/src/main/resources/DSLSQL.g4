
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
    : 'load' format '.' path ('options'|'where')? expression? booleanExpression* 'as' tableName
    | 'save' (overwrite | append | errorIfExists |ignore)* tableName 'as' format '.' path ('options'|'where')? expression? booleanExpression* (('partitionBy'|'partitionby') col? colGroup*)?
    | 'select' ~(';')* 'as' tableName
    | 'insert' ~(';')*
    | 'create' ~(';')*
    | 'drop' ~(';')*
    | 'refresh' ~(';')*
    | 'set' setKey '=' setValue ('options'|'where')? expression? booleanExpression*
    | 'connect' format ('options'|'where')? expression? booleanExpression* ('as' db)?
    | ('train'|'run'|'predict') tableName 'as' format '.' path ('options'|'where')? expression? booleanExpression* asTableName*
    | 'register' format '.' path 'as' functionName ('options'|'where')? expression? booleanExpression*
    | 'unregister' format '.' path ('options'|'where')? expression? booleanExpression*
    | 'include' format '.' path ('options'|'where')? expression? booleanExpression*
    |  SIMPLE_COMMENT
    ;

overwrite
    : 'overwrite'
    ;

append
    : 'append'
    ;

errorIfExists
    : 'errorIfExists'
    ;

ignore
    : 'ignore'
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
