package org.monitoring.queryapi.preaggregation;

import com.google.code.morphia.Datastore;
import com.google.code.morphia.Morphia;
import com.mongodb.BasicDBObject;
import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MapReduceCommand;
import com.mongodb.MapReduceOutput;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.monitoring.queryapi.Event;
import org.monitoring.queryapi.Field;
import org.monitoring.queryapi.Manager;

/**
 *
 * @author Michal Dubravcik
 */
public class PreaggregateMongoMRI implements Preaggregate {

    private DBCollection col;
    String colName;
    DBObject allocateObject;
    Morphia morphia = new Morphia();
    String aggField = "agg";
    int countForMR = 20;

    public PreaggregateMongoMRI(DBCollection col) {
        this.col = col;
        colName = col.getName();
        col.createIndex(new BasicDBObject("date", 1));
    }
    
    public void setCountForMr(int count){
        countForMR = count;
    }
    
    public int getCountForMr(){
        return countForMR;
    }

    /**
     * Save event into DB and perform actualization of aggregations via incremental map-reduce
     *
     * @param unit
     * @param times
     * @param event
     */
    @Override
    public void saveEvent(TimeUnit unit, int[][] times, Event event) {
        
        morphia.map(Event.class);
        //col.save(morphia.toDBObject(event), WriteConcern.SAFE);
        Datastore ds = morphia.createDatastore(col.getDB().getMongo(), col.getDB().toString());
        ds.save(event);
        DBObject query = BasicDBObjectBuilder.start()
                .push("count").append(Field.LT, countForMR)
                .get();
        DBObject update = BasicDBObjectBuilder.start()
                .push("$inc").append("count", 1)
                .pop().push("$set").append("to", event.getId())
                .pop().push("$setOnInsert").append("from", event.getId())
                .get();
        DBObject counter = col.getDB().getCollection("counter")
                .findAndModify(query, null, new BasicDBObject("_id", -1), false, update, true, true);

        if (((Integer) counter.get("count")) % countForMR == 0) {
            //col.getDB().getCollection("counter").remove(new BasicDBObject("count", 10), WriteConcern.NORMAL);
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

                String map = "preaggregate_map_inc(this)";
                String reduce = "function(id, values){ return preaggregate_reduce_inc(id, values);}";


                BasicDBObjectBuilder queryLocal = new BasicDBObjectBuilder();
                queryLocal.push("_id")
                        .append(Field.GTE, counter.get("from"))
                        .append(Field.LTE, counter.get("to"));

                String range = "";
                if (rangeLeft != 0 || rangeRight != 0) {
                    range = ".l" + rangeLeft + ".r" + rangeRight;
                }
                String outputCol = colName + actual + range;

                MapReduceCommand mapReduceCmd =
                        new MapReduceCommand(col, map, reduce, outputCol,
                        MapReduceCommand.OutputType.REDUCE, queryLocal.get());

                Map<String, Object> globalVariables = new HashMap<String, Object>();
                globalVariables.put("actualMillis", unit.toMillis(actual));
                globalVariables.put("nextMillis", unit.toMillis(next));
                globalVariables.put("rangeLeft", rangeLeft);
                globalVariables.put("rangeRight", rangeRight);
                mapReduceCmd.setScope(globalVariables);
                mapReduceCmd.setSort(new BasicDBObject("_id", -1));
                mapReduceCmd.setLimit(countForMR);

                MapReduceOutput out = col.mapReduce(mapReduceCmd);
                int x = 2 + 1;

            }
        }
    }
}
