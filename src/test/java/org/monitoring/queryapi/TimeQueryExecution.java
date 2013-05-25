/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.monitoring.queryapi;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

/**
 *
 * @author Michal
 */
public class TimeQueryExecution {

    public static void main(String[] args) {
        int step = 1000;
        Manager m = new Manager();
        Query q = m.createQueryOnCollection("aggregate");
        int i = 0;
        
        StatisticsAggregationFramework st = new StatisticsAggregationFramework(q);
        System.out.println(st.groupingObjectForAF(new BasicDBObject("$sum", "value")));
        
        long start = System.nanoTime();
        Iterable<DBObject> it = (Iterable<DBObject>) q.setStep(86400000).orderDateAsc().sum("value").get("result");        
        long end = System.nanoTime();  
        System.out.println((end-start)/1e6);
        System.out.println("count " + i);
        
        q.setImplementation(new StatisticsMapReduce(q));
        start = System.nanoTime();
        it = (Iterable<DBObject>) q.setStep(172800000).sum("value").get("result");
        end = System.nanoTime(); 
        i=0;      
        System.out.println((end-start)/1e6);
        System.out.println("count " + i);
    }
}
