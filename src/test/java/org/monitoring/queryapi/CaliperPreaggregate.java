package org.monitoring.queryapi;

import com.google.caliper.Runner;
import com.google.caliper.SimpleBenchmark;
import com.google.code.morphia.Morphia;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.TimeUnit;
import org.monitoring.queryapi.preaggregation.Preaggregate;
import org.monitoring.queryapi.preaggregation.PreaggregateMongo;
import org.monitoring.queryapi.preaggregation.PreaggregateMongoMR;
import org.monitoring.queryapi.preaggregation.PreaggregateMongoMRI;
import org.monitoring.queryapi.preaggregation.compute.Compute;
import org.monitoring.queryapi.preaggregation.compute.ComputeAvg;

/**
 *
 * @author Michal Dubravcik
 */
public class CaliperPreaggregate extends SimpleBenchmark {

    static Manager m = new Manager();
    static List<Event> list = new ArrayList<Event>();
    static Morphia morphia = new Morphia();
    static DBCollection col = m.getDb().getCollection("aggregate");
    int from, to;
    Compute computer = new ComputeAvg();
    TimeUnit unit = TimeUnit.MINUTES;
    int[][] times = {{60, 24}, {1440, 30, 3, 3}, {1440, 30}, {43200, 12} };
    Preaggregate preaggregate = new PreaggregateMongo(col);
    Preaggregate preaggregateMR = new PreaggregateMongoMR(col);
    Preaggregate preaggregateMRI = new PreaggregateMongoMRI(col);

    @Override
    protected void setUp() {
        col = m.getDb().getCollection("aggregate");
        m.getDb().dropDatabase();
        m.getDb().createCollection("aggregate", new BasicDBObject("size", 1024*1024*10));
        col.createIndex(new BasicDBObject("date", 1));
        m.executeJSSaveFromDefaultFile();
        Calendar cal = new GregorianCalendar(2013, 1, 1, 1, 0, 0);
        cal.set(Calendar.MILLISECOND, 0);
        from = 0;
        to = 200;
        for (int i = from; i < to; i++) {
            Event event = new Event();
            event.setDate(cal.getTime());
            cal.setTime(new Date(cal.getTime().getTime() + 1000 * 60 * 15));
            event.setValue(10);
            list.add(event);
        }
    }

    @Override
    protected void tearDown() {
        //m.getDb().dropDatabase();
    }

    public void imeClassicAggregate(int reps) {
        for (int i = 0; i < reps; i++) {
            preaggregate.saveEvent(unit, times, list.get(i));
        }
    }

    public void timeMapReduceIncAggregate(int reps) {
        for (int i = 0; i < reps; i++) {
            preaggregateMRI.saveEvent(unit, times, list.get(i));
        }
    }

    public static void main(String[] args) throws Exception {
        Locale.setDefault(Locale.US);
        Runner.main(CaliperPreaggregate.class, args);
    }
}
