package org.monitoring.queryapi;

import com.mongodb.DBCollection;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.monitoring.queryapi.preaggregation.Preaggregate;
import org.monitoring.queryapi.preaggregation.PreaggregateMongoMRI;

/**
 * Preaggregation in threads
 * @author Michal Dubravcik
 */
public class SaveThread extends Thread {

    static Manager m = new Manager();
    Event e;
    DBCollection col;
    List<Event> list = new ArrayList<Event>();
    Preaggregate preaggregate;
    TimeUnit unit = TimeUnit.MINUTES;
    int[][] times = {{60, 1440}, {1440, 43200}};
    CountDownLatch latch;
    int i;

    SaveThread(int i,CountDownLatch latch, DBCollection col, List<Event> list) {
        this.latch = latch;
        this.col = col;
        this.list = list;
        this.i = i;
        preaggregate = new PreaggregateMongoMRI(col);
    }

    @Override
    public void run() {
        for (Event event : list) {
            preaggregate.saveEvent(unit, times, event);
        }
        latch.countDown();
    }
}
