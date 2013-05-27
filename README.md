
MongoDB Query API for Java
---------------------------
System contains 2 main parts : 
* **Query** providing ad-hoc quering functionality
* **Preaggregate** providing pre-computing of aggregations (statistics) known in advance

---------------------

**Install**

Running MongoDB instance is needed. Specify host and port in config.properties

**Run**
     
Use Manager class for instancing Query or Preaggregate. 

     Manager m = new Manager();
     Query logs = m.createQueryOnCollection("log");
     
Query uses fluent api - chaining of method calling can be used. Terminate the query with methods : find, distinct, countAll, count, min, max, avg, sum

This will returns matched logs by fields

     logs.field("source").equal("local").field("timeMillis").greaterThan(100).find();

Compute statistics like average - return avg times recorded in logs grouped 1 hour intervals

     logs.field("source").equal("local").step(36e5).field("timeMillis").avg();

Preaggregate uses only one method saveEvent(). Specify parameters base time unit eg. TimeUnit.DAY and then step time levels, implementation - how is statistic precomputed is in JS functions (if you use MapReduceIncremental implementation) or in PreaggregateCompute class (if you use basic upsert implementation

For saving events and actualizing daily statistics stored in month document (default sum,count,avg) invoke:

     preaggregate.saveEvent(TimeUnit.DAY, {{1,30}}, event};
     
or more complex preaggregation : save and actualize daily in month (1 day in 30 days, week sliding in month (1 day -3 day +3days in 30 days), monthly in year (30 days in 356 days):

     preaggregate.saveEvent(TimeUnit.DAY, {{1,30}, {1,30,3,3}, {30,356}}, event};
