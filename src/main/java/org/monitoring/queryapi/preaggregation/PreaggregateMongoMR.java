package org.monitoring.queryapi.preaggregation;

import com.google.code.morphia.Datastore;
import com.google.code.morphia.Morphia;
import com.mongodb.BasicDBObject;
import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MapReduceCommand;
import com.mongodb.MapReduceOutput;
import java.util.Date;
import java.util.concurrent.TimeUnit;
import org.monitoring.queryapi.Event;
import org.monitoring.queryapi.Field;

/**
 * Precounting of statistics via Map Reduce in Mongo
 * @author Michal Dubravcik
 */
public class PreaggregateMongoMR implements Preaggregate {

    private DBCollection col;
    String colName;
    DBObject allocateObject;
    Morphia morphia = new Morphia();
    String aggField = "agg";

    public PreaggregateMongoMR(DBCollection col) {
        this.col = col;
        colName = col.getName();       
        col.createIndex(new BasicDBObject("date", 1));
    }

    /**
     * Update aggregations with Event value(s). Method creates initial documents in DB if no are
     * found. Otherwise documents are updated base on PreaggregateCompute implementation.
     *
     * New collections are created with name of Preaggregate collection name from constructor and
     * suffixed with each element of times array.
     *
     * * * * *
     * Example: int[][] times = {{1,60},{24,30}}; saveEvent(TimeUnit.MINUTES, times, 1);
     *
     * Creates collection db.aggregate1 containing 60 fields that holds aggregations for each minute
     * in one day
     * db.aggregate24 fields that contains aggregations for daily aggregations stored in month document
     *
     * Use 3. and 4. element for sliding aggregation where 3. is left range and 4. right range
     * {{1,30,3,3}} with unit day, creates sliding week (3 left days, 1 day, 3 right days)
     * aggregation stored in month document
     * * * * *
     * Computing is based on Stored JS functions in Mongo
     * @param unit base time unit
     * @param times array of ints that create hierarchial structure of aggregations
     * @param event event from which values are taken
     */
    @Override
    public void saveEvent(TimeUnit unit, int[][] times, Event event) {
        Boolean fromBottom = true; 
        morphia.map(Event.class);
        Datastore ds = morphia.createDatastore(col.getDB().getMongo(), col.getDB().toString());
        ds.save(event);

        for (int i = 0; i < times.length; i++) {
            int before = 0;
            if (i > 0) {
                before = times[i - 1][0];
            }
            int actual = times[i][0];
            int next = times[i][1] * times[i][0];
            int rangeLeft = 0;
            if (times[i].length > 2) {
                rangeLeft = times[i][2];
            }
            int rangeRight = 0;
            if (times[i].length > 3) {
                rangeRight = times[i][3];
            }

            String map = "preaggregate_map(this)";
            String mapUpper = "preaggregate_map_upper(this)";
            String reduce = "function(id, values){ return preaggregate_reduce(id, values);}";

            for (int k = -rangeLeft; k <= rangeRight; k++) {
                Date start = new Date(event.getDate().getTime()
                        - event.getDate().getTime() % unit.toMillis(actual)
                        + unit.toMillis(actual * (k - rangeLeft)));
                Date middle = new Date(start.getTime()
                        + unit.toMillis(actual * (rangeLeft)));
                Date end = new Date(middle.getTime()
                        + unit.toMillis(actual * (rangeRight + 1)));

                BasicDBObjectBuilder queryLocal = new BasicDBObjectBuilder();
                queryLocal.push("date").append(Field.GTE, start).append(Field.LT, end);

                DBCollection inputCol;
                if (fromBottom == true || i == 0) {
                    inputCol = col;
                } else {
                    map = mapUpper;
                    reduce = ""; //REDUCE IS NOT INVOKED (ONLY MAP 1 TIME, WITHOUT REDUCE)
                    inputCol = col.getDB().getCollection("aggregate" + before);
                }
                MapReduceCommand mapReduceCmd =
                        new MapReduceCommand(inputCol, map, reduce, null,
                        MapReduceCommand.OutputType.INLINE, queryLocal.get());

                MapReduceOutput out = inputCol.mapReduce(mapReduceCmd);

                Long fieldTime = middle.getTime() % unit.toMillis(next)
                        / unit.toMillis(actual);

                String range = "";
                if (rangeLeft != 0 || rangeRight != 0 ){
                    range = ".l" + rangeLeft + ".r" + rangeRight;
                }
                DBCollection localCol = col.getDB()
                        .getCollection(colName + actual + range);

                Date aggDate = new Date(middle.getTime() - middle.getTime() % unit.toMillis(next));

                DBObject identificationOldDay = BasicDBObjectBuilder.start()
                        .append("date", aggDate)
                        .get();

                BasicDBObjectBuilder updateBuilder = new BasicDBObjectBuilder();
                DBObject ob = out.results().iterator().next();
                ob.removeField("_id");
                if (localCol.findOne(identificationOldDay) == null) {
                    BasicDBObjectBuilder builder = BasicDBObjectBuilder.start().push("$set");
                    for (Integer j = 0; j < next / actual; j++) {
                        builder.append(aggField + "." + j.toString(), allocateObject);
                    }
                    DBObject allocate = builder.get();
                    localCol.update(identificationOldDay, allocate, true, false);
                }
                updateBuilder.push("$set").append("agg." + fieldTime.toString(), (DBObject) ob.get("value"));

                DBObject update = updateBuilder.get();
                localCol.update(identificationOldDay, update, true, false);
            }
        }
    }
   
}
