# Kakfa Tool

## Fetch the latest N data in Kafka

```sql
!kafkaTool sampleData 10 records from "127.0.0.1:9092" wow;
```

## Infer the schema from kafka

```sql
!kafkaTool schemaInfer 10 records from "127.0.0.1:9092" wow;
```

## Check the stream checkpoint offset

```sql
!kakfaTool offsetStream /tmp/ck;
```
