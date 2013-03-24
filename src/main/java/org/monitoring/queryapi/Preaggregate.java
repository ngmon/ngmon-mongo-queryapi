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

    public void saveEventOld(TimeUnit unit, int[] time, MeterEvent event) {
        DBCollection colllectionYear = col.getDB().getCollection("aggregate.year");
        DBCollection collectionDay = col.getDB().getCollection("aggregate.day");
        col = col.getDB().getCollection("aggregate" + time[0]);


        Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone("GMT+1"));
        calendar.setTime(event.getDate());

        long event_time = event.getDate().getTime() - event.getDate().getTime() % unit.toMillis(time[0]);

        long event_year = event.getDate().getTime() - event.getDate().getTime() % TimeUnit.DAYS.toMillis(365); //TODO
        long event_month = event.getDate().getTime() - event.getDate().getTime() % TimeUnit.DAYS.toMillis(30); //TODO
        long event_week = event.getDate().getTime() - event.getDate().getTime() % TimeUnit.DAYS.toMillis(7);
        long event_day = event.getDate().getTime() - event.getDate().getTime() % TimeUnit.DAYS.toMillis(1);
        calendar.set(Calendar.MILLISECOND, 0);
        calendar.set(Calendar.SECOND, 0);
        calendar.set(Calendar.MINUTE, 0);
        calendar.set(Calendar.HOUR_OF_DAY, 0);
        long event_day2 = calendar.getTimeInMillis();
        long event_hours12 = event.getDate().getTime() - event.getDate().getTime() % TimeUnit.HOURS.toMillis(12);
        long event_hours4 = event.getDate().getTime() - event.getDate().getTime() % TimeUnit.HOURS.toMillis(4);
        long event_hours2 = event.getDate().getTime() - event.getDate().getTime() % TimeUnit.HOURS.toMillis(2);
        long event_hour = event.getDate().getTime() - event.getDate().getTime() % TimeUnit.HOURS.toMillis(1);
        long event_minutes30 = event.getDate().getTime() - event.getDate().getTime() % TimeUnit.MINUTES.toMillis(30);
        long event_minutes15 = event.getDate().getTime() - event.getDate().getTime() % TimeUnit.MINUTES.toMillis(15);

        DBObject identificationOldDay = BasicDBObjectBuilder.start().append("date", new Date(event_day)).get();
        //DBObject project = BasicDBObjectBuilder.start().append("date", 1).get();
        DBObject aggregatedDay = collectionDay.findOne(identificationOldDay);
        calendar.setTime(event.getDate());



        Integer fieldHourL = calendar.get(Calendar.HOUR_OF_DAY);
        Integer fieldMinuteL = calendar.get(Calendar.MINUTE) + fieldHourL * 60;



        if (aggregatedDay == null) {                            //allocate empty agregation document
            BasicDBObjectBuilder builder = BasicDBObjectBuilder.start().push("$set");
            for (Integer i = 0; i < 1440; i++) {
                builder.append("MINUTES." + i.toString(), new Double("0.0"));
            }
            for (Integer i = 0; i < 24; i++) {
                builder.append("HOURS." + i.toString(), new Double("0.0"));
            }
            DBObject allocate = builder.get();
            collectionDay.update(identificationOldDay, allocate, true, false);
        }

        BasicDBObjectBuilder updateBuilder = new BasicDBObjectBuilder();

        String timeField = fieldMinuteL.toString();
        Double value = new Double(0);
        /* get old aggregate object */
        if (aggregatedDay != null) {
            value = (Double) ((DBObject) aggregatedDay.get("MINUTES")).get(timeField);
        }
        /* recompute aggregated value */
        value = value + event.getValue();
        /* update aggregate object */
        updateBuilder.push("$set").append("MINUTES." + timeField, value);

        timeField = fieldHourL.toString();
        value = new Double(0);
        /* get old aggregate object */
        if (aggregatedDay != null) {
            value = (Double) ((DBObject) aggregatedDay.get("HOURS")).get(timeField);
        }
        /* recompute aggregated value */
        value = value + event.getValue();
        /* update aggregate object */
        updateBuilder.append("HOURS." + timeField, value);

        DBObject update = updateBuilder.get();
        collectionDay.update(identificationOldDay, update, true, false);
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
