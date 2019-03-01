# PG Extract And Replay

_A poor man's PostgreSQL load generator_

_plain and simple_

## Prerequisites
To tune / profile / stress(load) test a particular postgresql instance you need a postgresql log with queries. The most convinient way to get it is to configure postgresql instance to log every query that executes longer that 2 seconds (it's just an example)

## Step 0
```
groovy Extract.groovy postgresql-Fri.log postgresql.sql
```

This command will parse postgesql's log and will generate a file with queries. Most important point of this is that in the end you will have a list of queries that are ready to be execute on same postgresql instance. Queries will have paramters in their places.

## Step 1

```
groovy Replay.groovy jdbc:postgresql://localhost:5432/ login password postgresql.sql
```

This command will execute queries saved to postgresql.sql in a multithreaded way. As a result you will get a result.csv in current directory with information about queries execution.

## Step 2

You can gather descriptive statistic via your favourite tool or you can use Histrogram.groovy

## Step 3

After `Step 2` you'll have result.csv file. Every line of that file will contain a query id and it's execution time. But due to multithreaded execution order of queries in a result.csv usually doesn't correspond to postgresql.sql queries order. So to find what is a query with id 32 for example you could use a `QueryIndex.groovy`
