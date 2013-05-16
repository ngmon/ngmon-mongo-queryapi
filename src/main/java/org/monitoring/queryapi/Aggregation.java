package org.monitoring.queryapi;

/**
 * Project constants
 * @author Michal
 */
public class Aggregation {    
    //pipeline identifiers
    public static final String GROUP = "$group";
    public static final String SORT = "$sort";
    public static final String PROJECT = "$project";
    public static final String LIMIT = "$limit";
    public static final String MATCH = "$match";
    
    //pipeline operations
    public static final String SUM = "$sum";
    public static final String AVG = "$avg";
    public static final String MIN = "$min";
    public static final String MAX = "$max";
    
    public static final String OR = "$or";
}
