package org.monitoring.queryapi.preaggregation;

import org.monitoring.queryapi.preaggregation.compute.ComputeAvg;
import org.monitoring.queryapi.preaggregation.compute.Compute;
import com.google.code.morphia.Datastore;
import com.google.code.morphia.Morphia;
import com.mongodb.BasicDBObject;
import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.WriteResult;
import java.util.Date;
import java.util.concurrent.TimeUnit;
import org.monitoring.queryapi.Event;
import org.monitoring.queryapi.Manager;

/**
 * Precounting of statistics via upsert in Mongo
 * @author Michal Dubravcik
 */
public class PreaggregateMongo implements Preaggregate {

    private DBCollection col;
    String colName;
    DBObject allocateObject;
    Morphia morphia = new Morphia();
    String aggField = "agg";

    public PreaggregateMongo(DBCollection col) {
        this.col = col;
        colName = col.getName();
        allocateObject = (DBObject) com.mongodb.util.JSON.parse(Manager.readFile("src/main/resources/js_allocate.js"));
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
     *
     * @param unit base time unit
     * @param times array of ints that create hierarchial structure of aggregations
     * @param event event from which values are taken
     */
    @Override
    public void saveEvent(TimeUnit unit, int[][] times, Event event) {
        Long fieldTime;
        WriteResult wr;
        int i = 0;
        Compute computer = new ComputeAvg();

        morphia.map(Event.class);
        Datastore ds = morphia.createDatastore(col.getDB().getMongo(), col.getDB().toString());
        ds.save(event);

        while (i < times.length) {
            int timeActual = times[i][0];
            int timeNext = times[i][1] * times[i][0];
            int range = 0;
            if (times[i].length > 2) {
                range += times[i][2];
            }
            if (times[i].length > 3) {
                range += times[i][3];
            }
            i++;

            String colRange = "";
            if (range != 0) {
                colRange = ".r" + range;
            }
            DBCollection localCol = col.getDB()
                    .getCollection(colName + timeActual + colRange);

            for (int k = 0; k <= range; k++) {
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
                do {
                    DBObject aggregatedDoc = localCol.findOne(identificationOldDay, project);
                    
                    /* reset initial PreaggregateCompute fields  */
                    Compute comp = computer;
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
                    if (aggregatedDoc == null) {
                        aggregatedDoc = identificationOldDay;
                    }

                    wr = localCol.update(aggregatedDoc, update, true, false);
                } while (wr.getN() == 0); //succesful concurrent write
            }
        }
    }
}
