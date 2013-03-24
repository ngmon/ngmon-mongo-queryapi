package org.monitoring.queryapi;

import com.mongodb.BasicDBObject;
import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.Locale;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

/**
 *
 * @author Michal Dubravcik
 */
public class Preaggregate {

    private DBCollection col;

    public Preaggregate(DBCollection col) {
        this.col = col;
    }

    public void saveEvent(TimeUnit unit, int[] times, MeterEvent event) {
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
            col = col.getDB().getCollection("aggregate" + timeActual);

            long eventDate = event.getDate().getTime() - event.getDate().getTime() % unit.toMillis(timeNext);

            DBObject identificationOldDay = BasicDBObjectBuilder.start().append("date", new Date(eventDate)).get();
            //DBObject project = BasicDBObjectBuilder.start().append("date", 1).get();
            DBObject aggregatedDoc = col.findOne(identificationOldDay);

            fieldTime = event.getDate().getTime() % unit.toMillis(timeNext) / unit.toMillis(timeActual);

            if (aggregatedDoc == null) {                            //allocate empty agregation document
                BasicDBObjectBuilder builder = BasicDBObjectBuilder.start().push("$set");
                for (Integer j = 0; j < timeNext / timeActual; j++) {
                    builder.append(j.toString(), new Double("0.0"));
                }
                DBObject allocate = builder.get();
                col.update(identificationOldDay, allocate, true, false);
            }
            
            String fieldTimeString = fieldTime.toString();
            
            /* get old aggregate object */
            Double value = new Double(0);            
            if (aggregatedDoc != null) {
                value = (Double) aggregatedDoc.get(fieldTimeString);
            }
            
            /* recompute aggregated value */
            value = value + event.getValue();
            
            /* update aggregate object */
            BasicDBObjectBuilder updateBuilder = new BasicDBObjectBuilder();
            updateBuilder.push("$set").append(fieldTimeString, value);
            DBObject update = updateBuilder.get();
            
            col.update(identificationOldDay, update, true, false);
        }
    }
}
