### 编译DSL

如果修改了[DSLSQL.g4](../streamingpro-dsl/src/main/resources/DSLSQL.g4)文件，需要利用`antlr4`工具重新编译使其生效

### 生成DSL的相关源码

如果是`spark-2.3.0`以下的版本，使用`antlr4.5.3`
```bash
mvn antlr4:antlr4 -Pantlr4.5.3-maven -pl streamingpro-dsl-legacy
```

如果是`spark-2.3.0`以上的版本，使用`antlr4.7.1`
```bash
mvn antlr4:antlr4 -Pantlr4.7.1-maven -pl streamingpro-dsl
```
