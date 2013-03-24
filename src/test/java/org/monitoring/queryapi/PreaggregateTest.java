package org.monitoring.queryapi;

import com.google.code.morphia.Morphia;
import com.mongodb.DBCollection;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.BeforeClass;
import org.junit.Test;

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
        for (int i = 0; i < 100; i++) {
            Event event = new Event();
            event.setDate(new Date(new Date().getTime()));
            event.setValue(i);
            event.setValue2(1);
            Thread.sleep(10);
            list.add(event);
        }
    }

    @Test
    public void saveEvent() {
        Preaggregate preaggregate = new Preaggregate(col);
        for(Event event : list){
            int[] times = {1,15,60};
            preaggregate.saveEvent(TimeUnit.MINUTES, times, event);
        }
    }
}
