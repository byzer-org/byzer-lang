select "1" as a
as mtf1;

save overwrite mtf1
as carbondata.`visit_carbon`
options mode="overwrite"
and tableName="visit_carbon"
and implClass="org.apache.spark.sql.CarbonSource";