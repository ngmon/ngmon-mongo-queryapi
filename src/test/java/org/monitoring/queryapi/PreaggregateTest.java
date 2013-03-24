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
    static List<MeterEvent> list = new ArrayList<MeterEvent>();
    static Morphia morphia = new Morphia();
    static DBCollection col;

    @BeforeClass
    public static void setUp() throws InterruptedException {
        col = m.getDb().getCollection("aggregate");
        for (int i = 0; i < 100; i++) {
            MeterEvent event = new MeterEvent();
            event.setDate(new Date(new Date().getTime() + 60*1000*60));
            event.setValue(i);
            Thread.sleep(10);
            list.add(event);
        }
    }

    @Test
    public void saveEvent() {
        Preaggregate preaggregate = new Preaggregate(col);
        for(MeterEvent event : list){
            int[] times = {1,15,60};
            preaggregate.saveEvent(TimeUnit.MINUTES, times, event);
        }
    }
}
