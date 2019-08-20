# 动态创建UDF/UDAF

MLSQL 支持使用 Python和 Scala写UDF/UDAF。而且无需编译和打包或者重启，可以即时生效，
可以极大的方便用户增强SQL的功能。

这里对于Python UDF的支持值得注意的有如下几点：

1. python不支持任何native库，比如numpy.
2. python可能会有类型问题的坑，同时需要指定返回值。
3. 我们提供了专门的交互式python语法以及大规模数据处理的python语法，用以弥补python UDF的不足。在Python专门章节
   我们会提供更详细的介绍。

所以我们建议对于python尽可能只做简单的文本解析处理，以及使用原生自带的库。
目前MLSQL使用的是jython 2.7.1,更多细节可参考官网。