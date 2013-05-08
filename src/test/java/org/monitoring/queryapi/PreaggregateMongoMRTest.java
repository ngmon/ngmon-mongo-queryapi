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
import java.util.concurrent.TimeUnit;
import static org.junit.Assert.*;
import org.junit.BeforeClass;
import org.junit.Test;
import org.monitoring.queryapi.preaggregation.Preaggregate;
import org.monitoring.queryapi.preaggregation.PreaggregateMongoMR;

/**
 *
 * @author Michal Dubravcik
 */

public class PreaggregateMongoMRTest {

    static Manager m = new Manager();
    static List<Event> list = new ArrayList<Event>();
    static Morphia morphia = new Morphia();
    static DBCollection col;

    @BeforeClass
    public static void setUp() throws InterruptedException {
        col = m.getDb().getCollection("aggregate"); 
        m.getDb().dropDatabase();
        col.createIndex(new BasicDBObject("date", 1));
        Calendar cal = new GregorianCalendar(2013, 1, 1, 1, 0, 0);
        cal.set(Calendar.MILLISECOND, 0);
        for (int i = 0; i < 192; i++) {
            Event event = new Event();
            event.setDate(cal.getTime());
            cal.setTime(new Date(cal.getTime().getTime() + 1000*60*15)); 
            event.setValue(10);
            list.add(event);
        }
        
    }

    @Test
    public void saveEvent() {
        Preaggregate preaggregate = new PreaggregateMongoMR(col);
        TimeUnit unit = TimeUnit.MINUTES;
        int[][] times = {{1440, 10080}}; // dayily(in week)       
        Date start = new Date();
        for(Event event : list){
            preaggregate.saveEvent(unit, times, event);                        
        }
        Date end = new Date();
        System.out.println("Total ms: " +  (end.getTime() - start.getTime()));
        DBCollection c = m.getDb().getCollection("aggregate1440.l1.r1");
        Calendar cal = new GregorianCalendar(2013, 0, 31, 1, 0, 0);
        Date d = cal.getTime();
        DBObject doc = c.findOne(new BasicDBObject("date", cal.getTime()));        
        assertNotNull("empty response from DB aggregate1440.l1.r1", doc);
        assertEquals(new Double(96), (Double) ((DBObject)(((DBObject)doc.get("agg")).get("0"))).get("count"));
        assertEquals(new Double(192), (Double) ((DBObject)(((DBObject)doc.get("agg")).get("1"))).get("count"));
        assertEquals(new Double(192), (Double) ((DBObject)(((DBObject)doc.get("agg")).get("2"))).get("count"));
        assertEquals(new Double(96), (Double) ((DBObject)(((DBObject)doc.get("agg")).get("3"))).get("count"));
    }
}
