package org.monitoring.queryapi.preaggregation;

import com.google.code.morphia.Datastore;
import com.google.code.morphia.Morphia;
import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MapReduceCommand;
import com.mongodb.MapReduceOutput;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.monitoring.queryapi.Event;
import org.monitoring.queryapi.Manager;
import org.monitoring.queryapi.preaggregation.postgre.PostgreSQLDatabase;
import org.monitoring.queryapi.preaggregation.postgre.PostgreSQLDatabaseMapper;

/**
 *
 * @author Michal Dubravcik
 */
public class PreaggregateMongo implements Preaggregate {

    private DBCollection col;
    String colName;
    DBObject allocateObject;
    Morphia morphia = new Morphia();
    String aggField = "agg";
    PostgreSQLDatabase postgre = new PostgreSQLDatabase();

    public PreaggregateMongo(DBCollection col) {
        this.col = col;
        colName = col.getName();
        allocateObject = (DBObject) com.mongodb.util.JSON.parse(Manager.readFile("src/main/resources/js_allocate.js"));
    }

    /**
     * Update aggregations with Event value(s). Method creates initial documents in DB if no are
     * found. Otherwise documents are updated base on PreaggregateCompute implementation.
     *
     * New collections are created with name of Preaggregate collection name from constructor and
     * suffixed with each element of times array.
     *
     * * * * *
     * Example: int[] times = {1,60,1440}; saveEvent(TimeUnit.MINUTES, times, 1, --, --);
     *
     * Creates collection db.aggregate1, db.aggregate60, db.aggregate1440. db.aggregate60 contains
     * 60 fields that holds aggregations for each minute (first elem of array = 1 minute).
     * db.aggregate24 contains 24 (1440/60) fields that holds aggregations for each hour (second
     * elem of array = 60 minutes = 1 hour). db.aggregate1 contains only 1 (as it is last) field
     * that holds aggregations for each day (third elem of array = 1440 minutes = 24 hours = 1 day).
     *
     * Third parameter specifies that only 1 minute (hour,day) is updated with new event. Use number
     * 7 if you want that 1 minute (hour/day) holds 7 minute (hour/day) aggregations [-3,+3 units].
     * * * * *
     *
     * @param unit base time unit
     * @param times array of ints that create hierarchial structure of aggregations
     * @param range range specifing length (odd number) of interval around base date = (date -
     * range/2 ; date + range/2) in base units in which are aggregation updated
     * @param comp implementation specifing what and how to update with Event values
     * @param event event from which values are taken
     */
    @Override
    public void saveEvent(TimeUnit unit, int[][] times, Event event) {
        Long fieldTime;
        int i = 0;
        PreaggregateCompute computer = new PreaggregateComputeAvg();

        morphia.map(Event.class);
        Datastore ds = morphia.createDatastore(col.getDB().getMongo(), col.getDB().toString());
        ds.save(event);

        while (i < times.length) {
            int timeActual = times[i][0];
            int timeNext = times[i][1];
            int rangeLeft = 0;
            if (times[i].length > 2) {
                rangeLeft = times[i][2];
            }
            int rangeRight = 0;
            if (times[i].length > 3) {
                rangeRight = times[i][3];
            }
            i++;

            DBCollection localCol = col.getDB().getCollection(colName + timeActual);

            for (int k = -rangeLeft; k <= rangeRight; k++) {
                long difference = unit.toMillis(timeActual * k);
                long eventDateLocal = event.getDate().getTime() + difference;

                long eventDate = eventDateLocal - eventDateLocal % unit.toMillis(timeNext);

                DBObject identificationOldDay = BasicDBObjectBuilder.start()
                        .append("date", new Date(eventDate))
                        .get();

                fieldTime = eventDateLocal % unit.toMillis(timeNext) / unit.toMillis(timeActual);
                String fieldTimeString = fieldTime.toString();

                DBObject project = BasicDBObjectBuilder.start()
                        .append("date", 1).append("_id", 1).append(aggField, 1)
                        .get();

                DBObject aggregatedDoc = localCol.findOne(identificationOldDay, project);

                /* reset initial PreaggregateCompute fields  */
                PreaggregateCompute comp = computer;
                try {
                    comp = computer.getClass().newInstance();
                } catch (InstantiationException ex) {
                } catch (IllegalAccessException ex) {
                }

                java.lang.reflect.Field[] fields = comp.getClass().getFields();

                /* allocate empty agregation document */
                if (aggregatedDoc == null) {
                    BasicDBObjectBuilder builder = new BasicDBObjectBuilder();
                    for (java.lang.reflect.Field field : fields) {
                        try {
                            builder.append(field.getName(), field.get(comp));
                        } catch (IllegalArgumentException ex) {
                        } catch (IllegalAccessException ex) {
                        }
                    }
                    DBObject allocationPoint = builder.get();
                    builder = BasicDBObjectBuilder.start().push("$set");
                    for (Integer j = 0; j < timeNext / timeActual; j++) {
                        builder.append(aggField + "." + j.toString(), allocationPoint);
                    }
                    DBObject allocate = builder.get();
                    localCol.update(identificationOldDay, allocate, true, false);
                } else {
                    /* read fields from db and insert them into PreaggregateCompute instance */
                    for (java.lang.reflect.Field field : fields) {
                        try {
                            comp.getClass().getField(field.getName())
                                    .set(comp, ((DBObject) ((DBObject) aggregatedDoc.get(aggField)).get(fieldTimeString))
                                    .get(field.getName()));
                        } catch (IllegalArgumentException ex) {
                        } catch (IllegalAccessException ex) {
                        } catch (NoSuchFieldException ex) {
                        } catch (SecurityException ex) {
                        }
                    }
                }

                /* recompute aggregations */
                comp.recompute(event);

                /* create updating aggregation document */
                BasicDBObjectBuilder updateBuilder = new BasicDBObjectBuilder();
                updateBuilder.push("$set");
                for (java.lang.reflect.Field field : fields) {
                    try {
                        updateBuilder.append(aggField + "." + fieldTimeString + "." + field.getName(), field.get(comp));
                    } catch (IllegalArgumentException ex) {
                    } catch (IllegalAccessException ex) {
                    }
                }

                DBObject update = updateBuilder.get();

                localCol.update(identificationOldDay, update, true, false);
            }
        }
    }

    
}
