package org.monitoring.queryapi;

import com.google.code.morphia.Datastore;
import com.google.code.morphia.Morphia;
import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.List;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 *
 * @author Michal Dubravcik
 */
public class ReasonTest {
    
    static Manager m = new Manager();
    static List<Document> list = new ArrayList<Document>();
    static Morphia morphia = new Morphia();
    static DBCollection col;
    final static int NUM = 200;
    
    
    @BeforeClass
    public static void setUp() {
        m.setCollection("querytest");
        col = m.getCollection();
        col.drop();
        morphia.map(Document.class);
        Datastore ds = morphia.createDatastore(m.getDb().getMongo(), m.getDb().toString());

        for (int i = 0; i < NUM; i++) {
            Document doc = new Document();
            Calendar cal = new GregorianCalendar(2013, 1, 13, 16, 0, i);
            DocumentData docData = new DocumentData();
            docData.setValue(i);
            docData.setSource(i % 20);
            docData.setPart(1 + i % 4);
            doc.setDate(cal.getTime());
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
    public void testReason(){
//        Document doc = new Document();
//        DocumentData docData = new DocumentData();
//        docData.setSource(19);
//        doc.setData(docData);
//        DBObject effect = morphia.toDBObject(doc); // value, part fields NOT MAPPED AS EMPTY
        DBObject effect = BasicDBObjectBuilder.start().append("data.source", 19).get();
        DBObject res = m.createQueryOnCollection("querytest").reasonFor(effect, 5, 10000, "data.source");
        Iterable<DBObject> it = (Iterable<DBObject>) res.get("result");
    }

}
