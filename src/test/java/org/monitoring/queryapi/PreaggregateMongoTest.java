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

/**
 *
 * @author Michal Dubravcik
 */

public class PreaggregateMongoTest {

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
            cal.set(Calendar.HOUR_OF_DAY, i/60 + 15);
            event.setDate(cal.getTime());
            event.setValue(10);
            list.add(event);
        }
    }

    @Test
    public void saveEvent() {
        Preaggregate preaggregate = new PreaggregateMongo(col);
        for(Event event : list){
            int[][] times = {{60, 1440,0,0},{1440, 2880,0,0},{2880, 2880,0,0}};
            preaggregate.saveEvent(TimeUnit.MINUTES, times, event);
        }
        DBCollection c = m.getDb().getCollection("aggregate1440");
        Calendar cal = new GregorianCalendar(2013, 1, 2, 1, 0, 0);
        Date d = cal.getTime();
        DBObject doc = c.findOne(new BasicDBObject("date", cal.getTime()));
        assertNotNull("empty response from DB aggregate1440", doc);
        assertEquals(new Double(100), (Double) ((DBObject)(((DBObject)doc.get("agg")).get("0"))).get("count"));
        assertEquals(new Double(0), (Double) ((DBObject)(((DBObject)doc.get("agg")).get("1"))).get("count"));
        
        c = m.getDb().getCollection("aggregate60");
        cal = new GregorianCalendar(2013, 1, 2, 1, 0, 0);
        doc = c.findOne(new BasicDBObject("date", cal.getTime()));
        assertNotNull("empty response from DB aggregate60", doc);
        assertEquals(new Double(60), (Double) ((DBObject)(((DBObject)doc.get("agg")).get("14"))).get("count"));
        assertEquals(new Double(40), (Double) ((DBObject)(((DBObject)doc.get("agg")).get("15"))).get("count"));
        
        c = m.getDb().getCollection("aggregate2880");
        cal = new GregorianCalendar(2013, 1, 2, 1, 0, 0);
        doc = c.findOne(new BasicDBObject("date", cal.getTime()));
        assertNotNull("empty response from DB aggregate2880", doc);
        assertEquals(new Double(100), (Double) ((DBObject)(((DBObject)doc.get("agg")).get("0"))).get("count"));
    }
}
