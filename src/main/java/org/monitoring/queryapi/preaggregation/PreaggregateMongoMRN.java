package org.monitoring.queryapi.preaggregation;

import com.google.code.morphia.Datastore;
import com.google.code.morphia.Morphia;
import com.mongodb.BasicDBObject;
import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MapReduceCommand;
import com.mongodb.MapReduceOutput;
import com.mongodb.WriteConcern;
import java.awt.Cursor;
import java.text.DateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import org.monitoring.queryapi.Event;
import org.monitoring.queryapi.Field;
import org.monitoring.queryapi.Manager;

/**
 *
 * @author Michal Dubravcik
 */
public class PreaggregateMongoMRN implements Preaggregate {

    private DBCollection col;
    String colName;
    DBObject allocateObject;
    Morphia morphia = new Morphia();
    String aggField = "agg";

    public PreaggregateMongoMRN(DBCollection col) {
        this.col = col;
        colName = col.getName();
        allocateObject = (DBObject) com.mongodb.util.JSON.parse(Manager.readFile("src/main/resources/js_allocate.js"));
    }

    @Override
    public void saveEvent(TimeUnit unit, int[][] times, Event event) {
        int countForMR = 10;
        Boolean fromBottom = true;
        morphia.map(Event.class);
        DBObject object = morphia.toDBObject(event);
        col.save(object);
        col.getDB().getCollection("temp").save(object);

        /*DBObject query = BasicDBObjectBuilder.start()
         .push("to").append("$lte", event.getDate()).pop()
         .push("count").append("$lt", countForMR)
         .get();
         DBObject update = BasicDBObjectBuilder.start()
         .push("$inc").append("count", 1)
         .pop().push("$set").append("to", event.getDate())
         .pop().push("$setOnInsert").append("from", event.getDate())
         .get();
         DBObject counter = col.getDB().getCollection("counter")
         .findAndModify(query, null, new BasicDBObject("_id", -1), false, update, true, true);*/

        if (col.getDB().getCollection("temp").count() % countForMR == 0) {
            DBCollection temp = col.getDB().getCollection("temp");
            DBCollection temp2 = col.getDB().getCollection("temp" + event.getDate().getTime());
            DBObject o;
            while ((o = temp.findAndModify(null, null, null, true, null, true, false)) != null) {
                temp2.save(o);
            }
            //col.getDB().getCollection("counter").remove(new BasicDBObject("count", 10), WriteConcern.NORMAL);
            for (int i = 0; i < times.length; i++) {
                int before = 0;
                if (i > 0) {
                    before = times[i - 1][0];
                }
                int actual = times[i][0];
                int next = times[i][1];
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

                for (int k = -rangeLeft; k <= rangeRight; k++) {
                    BasicDBObjectBuilder queryLocal = new BasicDBObjectBuilder();
                    //queryLocal.push("date").append(Field.GTE, (Date) counter.get("from")).append(Field.LT, (Date) counter.get("to"));

                    String range = "";
                    if (rangeLeft != 0 || rangeRight != 0) {
                        range = ".l" + rangeLeft + ".r" + rangeRight;
                    }
                    String outputCol = colName + actual + range;
                    DBCollection inputCol = temp2;
                    MapReduceCommand mapReduceCmd =
                            new MapReduceCommand(inputCol, map, reduce, outputCol,
                            MapReduceCommand.OutputType.REDUCE, queryLocal.get());

                    Map<String, Object> globalVariables = new HashMap<String, Object>();
                    globalVariables.put("actualMillis", unit.toMillis(actual));
                    globalVariables.put("nextMillis", unit.toMillis(next));
                    mapReduceCmd.setScope(globalVariables);

                    MapReduceOutput out = inputCol.mapReduce(mapReduceCmd);
                    int x = 2 + 1;
                    inputCol.drop();
                }
            }
        }
    }
}
