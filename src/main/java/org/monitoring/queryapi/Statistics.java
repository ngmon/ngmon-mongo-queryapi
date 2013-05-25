package org.monitoring.queryapi;

import com.mongodb.DBObject;

/**
 *  Interface for statistical functions
 * 
 * @author Michal Dubravcik
 */
public interface Statistics {

    /**
     * Count of events
     */
    DBObject count();    
    
    /**
     * Average value of field in events
     */
    DBObject avg(String field);
    
    /**
     * Sum value of field in events
     */
    DBObject sum(String field);

    /**
     * Maximal value of field in events
     */
    DBObject max(String field);
    
    /**
     * Minimal value of field in events
     */
    DBObject min(String field);
    
}
