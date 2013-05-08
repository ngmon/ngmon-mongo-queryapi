package org.monitoring.queryapi;

import com.google.code.morphia.Datastore;
import com.google.code.morphia.Morphia;
import com.mongodb.DBCollection;
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
 *
 * @author Michal
 */
public class QueryTest {

    static Manager m = new Manager();
    static List<Document> list = new ArrayList<Document>();
    static Morphia morphia = new Morphia();
    static DBCollection col;
    final static int NUM = 20;

    

    @BeforeClass
    public static void setUp() {
        m.setCollection("querytest");
        col = m.getCollection();
        m.executeJSSaveFromDefaultFile();
        col.drop();
        morphia.map(Document.class);
        Datastore ds = morphia.createDatastore(m.getDb().getMongo(), m.getDb().toString());

        for (int i = 0; i < NUM; i++) {
            Document doc = new Document();
            Calendar cal = new GregorianCalendar(2013, 1, 13, 16, 0, i);
            DocumentData docData = new DocumentData();
            docData.setValue(i);
            doc.setTime(cal.getTime());
            doc.setData(docData);
            ds.save(doc);
            list.add(doc);
        }
    }

    @AfterClass
    public static void tearDown() {
        col.drop();
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
        DBObject res = (DBObject) q.setStep(10000).count().get("result");
        assertNotEquals("Empty result", 0, res.toMap().size());
        Iterator it = ((Iterable<DBObject>) res).iterator();
        DBObject ob = (DBObject) it.next();
        assertEquals("1. batch", new Double(NUM / 2), ob.get("value"));
        ob = (DBObject) it.next();
        assertEquals("2. batch", new Double(NUM / 2), ob.get("value"));
    }

    @Test
    public void avg() {
        Query q = m.createQueryOnCollection("querytest");
        int i = 0;
        DBObject res = (DBObject) q.setStep(10000).orderAsc("_id").avg("value").get("result");
        assertNotEquals("Empty result", 0, res.toMap().size());
        Iterator it = ((Iterable<DBObject>) res).iterator();
        DBObject ob = (DBObject) it.next();
        assertEquals("1. batch", new Double(4.5), ob.get("value"));
        ob = (DBObject) it.next();
        assertEquals("2. batch", new Double(14.5), ob.get("value"));
    }

    @Test
    public void min() {
        Query q = m.createQueryOnCollection("querytest");
        int i = 0;
        DBObject res = (DBObject) q.setStep(10000).min("value").get("result");
        Iterator it = ((Iterable<DBObject>) res).iterator();
        DBObject ob = (DBObject) it.next();
        assertEquals("1. batch", new Double(0), ob.get("value"));
        ob = (DBObject) it.next();
        assertEquals("2. batch", new Double(10), ob.get("value"));
    }

    @Test
    public void max() {
        Query q = m.createQueryOnCollection("querytest");
        int i = 0;
        DBObject res = (DBObject) q.setStep(10000).max("value").get("result");
        Iterator it = ((Iterable<DBObject>) res).iterator();
        DBObject ob = (DBObject) it.next();
        assertEquals("1. batch", new Double(9), ob.get("value"));
        ob = (DBObject) it.next();
        assertEquals("2. batch", new Double(19), ob.get("value"));
    }

    @Test
    public void median() {
        Query q = m.createQueryOnCollection("querytest");
        int i = 0;
        DBObject res = (DBObject) q.setStep(10000).median("value").get("result");
        assertNotEquals("Empty result", 0, res.toMap().size());
        Iterator it = ((Iterable<DBObject>) res).iterator();
        DBObject ob = (DBObject) it.next();
        assertEquals("1. batch", new Double(4.5), ob.get("value"));
        ob = (DBObject) it.next();
        assertEquals("2. batch", new Double(14.5), ob.get("value"));
    }

    @Test
    public void avgCached() {
        Query q = m.createQueryOnCollection("querytest");
        int i = 0;
        List res = (List) q.setStep(10000).avgCached("value").get("result");
        assertNotEquals("Empty result", 0, res.size());
        Iterator it = ((Iterable<DBObject>) res).iterator();
        DBObject ob = (DBObject) it.next();
        assertEquals("1. batch", new Double(4.5), ob.get("value"));
        ob = (DBObject) it.next();
        assertEquals("2. batch", new Double(14.5), ob.get("value"));
    }

    @Test
    public void countCached() {
        Query q = m.createQueryOnCollection("querytest");
        int i = 0;
        List res = (List) q.setStep(10000).countCached().get("result");
        assertNotEquals("Empty result", 0, res.size());
        Iterator it = ((Iterable<DBObject>) res).iterator();
        DBObject ob = (DBObject) it.next();
        assertEquals("1. batch", new Double(NUM / 2), ob.get("value"));
        ob = (DBObject) it.next();
        assertEquals("2. batch", new Double(NUM / 2), ob.get("value"));
    }

    @Test
    public void minCached() {
        Query q = m.createQueryOnCollection("querytest");
        int i = 0;
        List res = (List) q.setStep(10000).minCached("value").get("result");
        Iterator it = ((Iterable<DBObject>) res).iterator();
        DBObject ob = (DBObject) it.next();
        assertEquals("1. batch", new Double(0), ob.get("value"));
        ob = (DBObject) it.next();
        assertEquals("2. batch", new Double(10), ob.get("value"));
    }

    @Test
    public void maxCached() {
        Query q = m.createQueryOnCollection("querytest");
        int i = 0;
        List res = (List) q.setStep(10000).maxCached("value").get("result");
        Iterator it = ((Iterable<DBObject>) res).iterator();
        DBObject ob = (DBObject) it.next();
        assertEquals("1. batch", new Double(9), ob.get("value"));
        ob = (DBObject) it.next();
        assertEquals("2. batch", new Double(19), ob.get("value"));
    }

    @Test
    public void medianCached() {
        Query q = m.createQueryOnCollection("querytest");
        int i = 0;
        List res = (List) q.setStep(10000).medianCached("value").get("result");
        assertNotEquals("Empty result", 0, res.size());
        Iterator it = ((Iterable<DBObject>) res).iterator();
        DBObject ob = (DBObject) it.next();
        assertEquals("1. batch", new Double(4.5), ob.get("value"));
        ob = (DBObject) it.next();
        assertEquals("2. batch", new Double(14.5), ob.get("value"));
    }
    
    
}
