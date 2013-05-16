package org.monitoring.queryapi;

import com.mongodb.DBObject;

/**
 *  Interface for statistical functions
 * 
 * @author Michal Dubravcik
 */
public interface Statistics {

    
    DBObject count();    
    
    DBObject avg(String field);
    
    DBObject sum(String field);

    DBObject max(String field);
    
    DBObject min(String field);
    
}
