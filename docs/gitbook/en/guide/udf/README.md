# Create UDF/UDAF

You can write source code of Python,Scala,Java in MLSQL, without compile, package or deploy,restart. It works like SQL.
This means you can easy to enhance the SQL function.

Notice that the python UDF have some limitations:

1. No native library, e.g. numpy.
2. Only support python 2.7
3. You should tell the system the return value of python.

