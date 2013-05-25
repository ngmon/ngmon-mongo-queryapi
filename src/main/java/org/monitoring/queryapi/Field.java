package org.monitoring.queryapi;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

/**
 * Field constructor for matching documents in DB
 * 
 * @author Michal Dubravcik
 */
public class Field {
    
    public static final String NE = "$ne";
    public static final String GTE = "$gte";
    public static final String GT = "$gt";
    public static final String LTE = "$lte";
    public static final String LT = "$lt";
    
    //Query for FluentAPI
    Query query;
    //Key of field for matching
    String field;
            
    public Field(Query query, String field){
        this.query = query;
        this.field = field;
    }
    
    /**
     * Match documents that contains this field
     * @return 
     */
    public Query exists(){
        query.append(field, new BasicDBObject("$exists", true));
        return query;
    }
    
    /**
     * Match documents that does not contain this field
     * @return 
     */
    public Query doesNotExist(){
        query.append(field, new BasicDBObject("$exists", false));
        return query;
    }
    
    /**
     * Match documents whose field is equal to the value
     * @param value
     * @return 
     */
    public Query equal(Object value){
        query.append(field, value);
        return query;
    }
    
    /**
     * Match documents whose field is not equal to the value
     * @param value
     * @return 
     */
    public Query notEqual(Object value){
        query.append(field, new BasicDBObject(NE, value));
        return query;
    }
    
    /**
     * Match documents whose field is less than value
     * @param value
     * @return 
     */
    public Query lessThan(Object value){
        query.append(field, new BasicDBObject(LT, value));
        return query;
    }
    
    /**
     * Match documents whose field is less than or equal to value
     * @param value
     * @return 
     */
    public Query lessThanOrEq(Object value){
        query.append(field, new BasicDBObject(LTE, value));
        return query;
    }
    
    /**
     * * Match documents whose field is greater than value
     * @param value
     * @return 
     */
    public Query greaterThan(Object value){
        query.append(field, new BasicDBObject(GT, value));
        return query;
    }
    
    /**
     * Match documents whose field is greater than or equal to value
     * @param value
     * @return 
     */
    public Query greaterThanOrEq(Object value){
        query.append(field, new BasicDBObject(GTE, value));
        return query;
    }  
    
    /**
     * Match documents whose field is equal to one of values
     * @param values
     * @return 
     */
    public Query hasOneOf(Iterable<?> values){
        query.append(field, new BasicDBObject("$in", values));
        return query;
    }
    
    /**
     * Match documents whose array field contains all the values
     * @param values
     * @return 
     */
    public Query hasAllOf(Iterable<?> values){
        query.append(field, new BasicDBObject("$all", values));
        return query;
    }
    
    /**
     * Match documents whose array field does not contain any of values
     * @param values
     * @return 
     */
    public Query hasNoneOf(Iterable<?> values){
        query.append(field, new BasicDBObject("$nin", values));
        return query;
    }
    
    /**
     * Match documents whose array field contains value
     * @param value
     * @return 
     */
    public Query hasThisOne(Object value){
        return equal(value);
    }
    
    /**
     * Match documents whose array field holds value condition
     * @param value
     * @return 
     */
    public Query hasThisElement(Object value){
        query.append(field, new BasicDBObject("$elemMatch", value));
        return query;
    }
    
    public DBObject sum(){
        return query.sum(field);
    }
    
    public DBObject avg(){
        return query.avg(field);
    }
    
    public DBObject min(){
        return query.min(field);
    }
    
    public DBObject max(){
        return query.max(field);
    }
    
    public DBObject median(){
        return query.median(field);
    }
}
