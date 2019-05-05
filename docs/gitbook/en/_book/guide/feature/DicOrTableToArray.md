## DicOrTableToArray

Fancy tricks. DicOrTableToArray can convert dictionary file or table to array.
you can call a user-defined-function to get a array.


```sql
select explode(k) as word from (select p("dic1") as k)
as words_table;

select lower(word) from words_table
as array_table;

train newdata as DicOrTableToArray.`/tmp/model2` where 
`table.paths`="array_table" 
and `table.names`="dic2";

register DicOrTableToArray.`/tmp/model2` as p2;

-- where you can get dic2 array.
select p2("dic2")  as k

```