package org.monitoring.queryapi;

import com.google.code.morphia.Morphia;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
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
import org.monitoring.queryapi.preaggregation.PreaggregateMongoMRN;

/**
 *
 * @author Michal Dubravcik
 */

public class PreaggregateMongoMRNTest {

    static Manager m = new Manager();
    static List<Event> list = new ArrayList<Event>();
    static Morphia morphia = new Morphia();
    static DBCollection col;

    @BeforeClass
    public static void setUp() throws InterruptedException {
        col = m.getDb().getCollection("aggregate"); 
        m.getDb().dropDatabase();
        col.createIndex(new BasicDBObject("date", 1));
        m.executeJSSaveFromDefaultFile();
        Calendar cal = new GregorianCalendar(2013, 1, 1, 1, 0, 0);
        cal.set(Calendar.MILLISECOND, 0);
        for (int i = 0; i < 100; i++) {
            Event event = new Event();
            event.setDate(cal.getTime());
            cal.setTime(new Date(cal.getTime().getTime() + 1000*60*60*12)); 
            event.setValue(10);
            list.add(event);
        }
        
    }

    @Test
    public void saveEvent() {
        Preaggregate preaggregate = new PreaggregateMongoMRN(col);
        TimeUnit unit = TimeUnit.MINUTES;
        int[][] times = {{1440, 10080,0,0}}; // 1,1    
        Date start = new Date();
        for(Event event : list){
            preaggregate.saveEvent(unit, times, event);            
        }
        Date end = new Date();
        System.out.println("Total ms: " +  (end.getTime() - start.getTime()));
    }
}
