package org.monitoring.queryapi;

import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import java.util.Date;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author Michal Dubravcik
 */
public class Preaggregate {

    private DBCollection col;
    String colName;

    public Preaggregate(DBCollection col) {
        this.col = col;
        colName = col.getName();
    }

    public void saveEvent(TimeUnit unit, int[] times, Event event) {
        Long fieldTime;
        int i = 0;

        int[] tt = new int[times.length + 1];
        System.arraycopy(times, 0, tt, 0, times.length);
        tt[times.length] = times[times.length - 1];
        times = tt;

        while (i < times.length - 1) {
            int timeActual = times[i];
            int timeNext = times[i + 1];
            i++;
            col = col.getDB().getCollection(colName + timeActual);

            long eventDate = event.getDate().getTime() - event.getDate().getTime() % unit.toMillis(timeNext);

            DBObject identificationOldDay = BasicDBObjectBuilder.start().append("date", new Date(eventDate)).get();
            //DBObject project = BasicDBObjectBuilder.start().append("date", 1).get();
            DBObject aggregatedDoc = col.findOne(identificationOldDay);

            fieldTime = event.getDate().getTime() % unit.toMillis(timeNext) / unit.toMillis(timeActual);

            PreaggregateComputeMaxMin computer = new PreaggregateComputeMaxMin();
            java.lang.reflect.Field[] fields = computer.getClass().getFields();
            String fieldTimeString = fieldTime.toString();

            /* allocate empty agregation document */
            if (aggregatedDoc == null) {
                BasicDBObjectBuilder builder = new BasicDBObjectBuilder();
                for (java.lang.reflect.Field field : fields) {
                    try {
                        builder.append(field.getName(), field.get(computer));
                    } catch (IllegalArgumentException ex) {
                    } catch (IllegalAccessException ex) {
                    }
                }
                DBObject allocationPoint = builder.get();
                builder = BasicDBObjectBuilder.start().push("$set");
                for (Integer j = 0; j < timeNext / timeActual; j++) {
                    builder.append(j.toString(), allocationPoint);
                }
                DBObject allocate = builder.get();
                col.update(identificationOldDay, allocate, true, false);
            } else {
                for (java.lang.reflect.Field field : fields) {
                    try {
                        computer.getClass().getField(field.getName())
                                .set(computer,
                                ((DBObject) aggregatedDoc.get(fieldTimeString)).get(field.getName()));
                    } catch (IllegalArgumentException ex) {
                    } catch (IllegalAccessException ex) {
                    } catch (NoSuchFieldException ex) {
                    } catch (SecurityException ex) {
                    }
                }
            }

            computer.recompute(event);

            /* update aggregate object */
            BasicDBObjectBuilder updateBuilder = new BasicDBObjectBuilder();
            updateBuilder.push("$set");
            for (java.lang.reflect.Field field : fields) {
                try {
                    updateBuilder.append(fieldTimeString + "." + field.getName(), field.get(computer));
                } catch (IllegalArgumentException ex) {
                } catch (IllegalAccessException ex) {
                }
            }

            DBObject update = updateBuilder.get();

            col.update(identificationOldDay, update, true, false);
        }
    }
}
