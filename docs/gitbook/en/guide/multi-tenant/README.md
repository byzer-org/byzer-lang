# Multi tenant

MLSQL Engine supports multi tenant. When you use MLSQL Engine alone, it's disabled by default. Once you enable it,
there are some features you should know:

1. All users share the resource of one MLSQL Engine.
2. Temp tables are not visible between users.
3. Every login user have his own HOME directory.
4. UDF created by users are not visible from each other. 