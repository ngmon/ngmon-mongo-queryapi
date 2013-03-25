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

public class PreaggregateTest {

    static Manager m = new Manager();
    static List<Event> list = new ArrayList<Event>();
    static Morphia morphia = new Morphia();
    static DBCollection col;

    @BeforeClass
    public static void setUp() throws InterruptedException {
        col = m.getDb().getCollection("aggregate");
        m.getDb().dropDatabase();
        Calendar cal = new GregorianCalendar(2013, 1, 2, 15, 0, 0);
        cal.set(Calendar.MILLISECOND, 0);
        for (int i = 0; i < 100; i++) {
            Event event = new Event();
            cal.set(Calendar.SECOND, i%60);
            cal.set(Calendar.MINUTE, i/60);
            event.setDate(cal.getTime());
            event.setValue(10);
            list.add(event);
        }
    }

    @Test
    public void saveEvent() {
        Preaggregate preaggregate = new Preaggregate(col);
        PreaggregateCompute computer = new PreaggregateComputeAvg();
        for(Event event : list){
            int[] times = {1, 60, 1440};
            preaggregate.saveEvent(TimeUnit.MINUTES, times, 1, computer, event);
        }
        DBCollection c = m.getDb().getCollection("aggregate60");
        Calendar cal = new GregorianCalendar(2013, 1, 2, 15, 0, 0);
        Date d = cal.getTime();
        DBObject doc = c.findOne(new BasicDBObject("date", cal.getTime()));
        assertNotNull("empty response from DB aggregate60", doc);
        assertEquals(new Double(60), (Double) ((DBObject)doc.get("0")).get("count"));
        assertEquals(new Double(40), (Double) ((DBObject)doc.get("1")).get("count"));
        
        c = m.getDb().getCollection("aggregate24");
        cal = new GregorianCalendar(2013, 1, 2, 1, 0, 0);
        doc = c.findOne(new BasicDBObject("date", cal.getTime()));
        assertNotNull("empty response from DB aggregate24", doc);
        assertEquals(new Double(100), (Double) ((DBObject)doc.get("14")).get("count"));
        
        c = m.getDb().getCollection("aggregate1");
        cal = new GregorianCalendar(2013, 1, 2, 1, 0, 0);
        doc = c.findOne(new BasicDBObject("date", cal.getTime()));
        assertNotNull("empty response from DB aggregate1", doc);
        assertEquals(new Double(100), (Double) ((DBObject)doc.get("0")).get("count"));
    }
}