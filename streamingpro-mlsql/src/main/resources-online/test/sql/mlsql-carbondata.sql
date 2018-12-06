select "1" as a
as mtf1;

save overwrite mtf1
as carbondata.`-`
options mode="overwrite"
and tableName="${tableName}"
and implClass="org.apache.spark.sql.CarbonSource";
