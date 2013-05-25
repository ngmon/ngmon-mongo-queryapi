package org.monitoring.queryapi;

import com.google.code.morphia.Datastore;
import com.google.code.morphia.Morphia;
import com.mongodb.DBObject;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.Iterator;
import java.util.List;
import org.junit.AfterClass;
import static org.junit.Assert.*;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * If needed change implementation via Manager
 * @author Michal
 */
public class QueryTest {

    static Manager m = new Manager();
    static List<Document> list = new ArrayList<Document>();
    static Morphia morphia = new Morphia();
    final static int NUM = 20;

    

    @BeforeClass
    public static void setUp() {
        m.setImplementation(Manager.Mode.MapReduceCached);
        System.out.println(m.MODE);
        m.getDb().dropDatabase();
        m.executeJSSaveFromDefaultFile();
        
        morphia.map(Document.class);
        Datastore ds = morphia.createDatastore(m.getDb().getMongo(), m.getDb().toString());

        for (int i = 0; i < NUM; i++) {
            Document doc = new Document();
            Calendar cal = new GregorianCalendar(2013, 1, 13, 16, 0, i);
            DocumentData docData = new DocumentData();
            docData.setValue(i);
            doc.setDate(cal.getTime());
            doc.setData(docData);
            ds.save(doc);
            list.add(doc);
        }
    }

    @AfterClass
    public static void tearDown() {
        //m.dropCollection("querytest");
    }

    @Test
    public void find() {
        Query q = m.createQueryOnCollection("querytest");
        int i = 0;
        for (DBObject ob : (Iterable<DBObject>) q.orderAsc("_id").find().get("result")) {
            assertEquals("find - returned objects are not same", list.get(i), morphia.fromDBObject(Document.class, ob));
            i++;
        }
    }

    @Test
    public void distinct() {
        Query q = m.createQueryOnCollection("querytest");
        int i = 0;
        for (int ob : (Iterable<Integer>) q.distinct("value").get("result")) {
            assertEquals("distinct - different values", list.get(i).getData().getValue(), ob);
            i++;
        }
    }

    @Test
    public void countAll() {
        Query q = m.createQueryOnCollection("querytest");
        int i = 0;
        assertEquals("count", list.size(), q.countAll());
    }

    @Test
    public void count() {
        Query q = m.createQueryOnCollection("querytest");
        int i = 0;
        Iterable<DBObject> res = (Iterable<DBObject>) q.setStep(10000).count().get("result");
        Iterator it = ((Iterable<DBObject>) res).iterator();
        DBObject ob = (DBObject) it.next();
        assertEquals("1. batch", new Double(NUM / 2), Double.valueOf(ob.get("value").toString()));
        ob = (DBObject) it.next();
        assertEquals("2. batch", new Double(NUM / 2), Double.valueOf(ob.get("value").toString()));
    }
    
    @Test
    public void sum() {
        Query q = m.createQueryOnCollection("querytest");
        int i = 0;
        Iterable<DBObject> res = (Iterable<DBObject>) q.setStep(10000).orderDateAsc().sum("data.value").get("result");
        Iterator it = ((Iterable<DBObject>) res).iterator();
        DBObject ob = (DBObject) it.next();
        assertEquals("1. batch", new Double(45), Double.valueOf(ob.get("value").toString()));
        ob = (DBObject) it.next();
        assertEquals("2. batch", new Double(145), Double.valueOf(ob.get("value").toString()));
    }

    @Test
    public void avg() {
        Query q = m.createQueryOnCollection("querytest");
        int i = 0;
        Iterable<DBObject> res = (Iterable<DBObject>) q.setStep(10000).orderAsc("_id").avg("data.value").get("result");
        Iterator it =  res.iterator();
        DBObject ob = (DBObject) it.next();
        assertEquals("1. batch", new Double(4.5), Double.valueOf(ob.get("value").toString()));
        ob = (DBObject) it.next();
        assertEquals("2. batch", new Double(14.5), Double.valueOf(ob.get("value").toString()));
    }

    @Test
    public void min() {
        Query q = m.createQueryOnCollection("querytest");
        int i = 0;
        Iterable<DBObject> res = (Iterable<DBObject>) q.setStep(10000).orderAsc("_id").min("data.value").get("result");
        Iterator it =  res.iterator();
        DBObject ob = (DBObject) it.next();
        assertEquals("1. batch", new Double(0), Double.valueOf(ob.get("value").toString()));
        ob = (DBObject) it.next();
        assertEquals("2. batch", new Double(10), Double.valueOf(ob.get("value").toString()));
    }

    @Test
    public void max() {
        Query q = m.createQueryOnCollection("querytest");
        int i = 0;
        Iterator it = ((Iterable<DBObject>) q.setStep(10000).orderAsc("_id").max("data.value").get("result")).iterator();
        DBObject ob = (DBObject) it.next();
        assertEquals("1. batch", new Double(9), Double.valueOf(ob.get("value").toString()));
        ob = (DBObject) it.next();
        assertEquals("2. batch", new Double(19), Double.valueOf(ob.get("value").toString()));
    }

    /*@Test NOT IMPLEMENTED, NEEDED TO BE COMPUTED IN APPLICATION
    public void median() {
        Query q = m.createQueryOnCollection("querytest");
        int i = 0;
        Iterable<DBObject> res = (Iterable<DBObject>) q.setStep(10000).median("data.value").get("result");
        Iterator it = res.iterator();
        DBObject ob = (DBObject) it.next();
        assertEquals("1. batch", new Double(4.5), ob.get("value"));
        ob = (DBObject) it.next();
        assertEquals("2. batch", new Double(14.5), ob.get("value"));
    }*/

    
}
