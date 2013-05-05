package org.monitoring.queryapi;

import com.google.code.morphia.Morphia;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.CountDownLatch;

/**
 *
 * @author Michal Dubravcik
 */
public class RunSaveThreads {
    
    static Manager m = new Manager();
    static List<Event> list = new ArrayList<Event>();
    static Morphia morphia = new Morphia();
    static DBCollection col;
        
    public static void main(String[] args) throws InterruptedException {
        
        col = m.getDb().getCollection("aggregate"); 
        m.getDb().dropDatabase();
        col.createIndex(new BasicDBObject("date", 1));
        m.executeJSSaveFromDefaultFile();
        Calendar cal = new GregorianCalendar(2013, 1, 1, 1, 0, 0);
        cal.set(Calendar.MILLISECOND, 0);
        for (int i = 0; i < 5000; i++) {
            Event event = new Event();
            event.setDate(cal.getTime());
            cal.setTime(new Date(cal.getTime().getTime() + 1000*60*60*12)); 
            event.setValue(10);
            list.add(event);
        }
        
        final CountDownLatch latch = new CountDownLatch(10);
        long startTime = System.nanoTime();
        for(int i=0; i < 10; i++){
            List<Event> l = new ArrayList<Event>();
            for(int j = 0; j < 20; j++){
                l.add(list.get(j*10 + i));
            }
            SaveThread s1 = new SaveThread(latch, col,l);
            s1.start();
        }       
        
        latch.await();
        long finishTime = System.nanoTime();
        System.out.println("Have passed : " + (finishTime - startTime)/1e9);
    } 

}
