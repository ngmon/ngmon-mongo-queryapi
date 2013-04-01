package org.monitoring.queryapi;

import com.google.code.morphia.Morphia;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 *
 * @author Michal Dubravcik
 */

public class PreaggregateTestMR {

    static Manager m = new Manager();
    static List<Event> list = new ArrayList<Event>();
    static Morphia morphia = new Morphia();
    static DBCollection col;

    @BeforeClass
    public static void setUp() throws InterruptedException {
        col = m.getDb().getCollection("aggregate"); 
        m.getDb().dropDatabase();
        col.createIndex(new BasicDBObject("date", 1));
        m.executeJSFromDefaultFile();
        Calendar cal = new GregorianCalendar(2013, 1, 2, 15, 0, 0);
        cal.set(Calendar.MILLISECOND, 0);
        for (int i = 999; i < 1000; i++) {
            Event event = new Event();
            cal.set(Calendar.MINUTE, i%60);
            cal.set(Calendar.DAY_OF_WEEK, i/300%7);
            cal.set(Calendar.HOUR_OF_DAY, i/60%24 + 3);  //HOUR OF DAY
            event.setDate(cal.getTime());
            event.setValue(10);
            list.add(event);
        }
        
    }

    @Test
    public void saveEvent() {
        Preaggregate preaggregate = new Preaggregate(col);
        TimeUnit unit = TimeUnit.MINUTES;
        int[] times = {/*60,*/ 1440, 10080}; // hourly(in day), dayily(in week)        
        //PreaggregateCompute computer = new PreaggregateComputeAvg();
        for(Event event : list){
            preaggregate.saveEventMR(unit, times, 3, 3, event, false);            
            //preaggregate.saveEvent(TimeUnit.MINUTES, times, 1, computer, event);
        }
        DBCollection c = m.getDb().getCollection("aggregate60");
        Calendar cal = new GregorianCalendar(2013, 2, 9, 15, 0, 0);
        Date d = cal.getTime();
        DBObject doc = c.findOne(new BasicDBObject("date", cal.getTime()));
    }
}
