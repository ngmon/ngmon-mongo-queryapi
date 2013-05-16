package org.monitoring.queryapi;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

/**
 *
 * @author Michal Dubravcik
 */
public class Field {
    
    public static final String NE = "$ne";
    public static final String GTE = "$gte";
    public static final String GT = "$gt";
    public static final String LTE = "$lte";
    public static final String LT = "$lt";
    
    Query query;
    String field;
            
    public Field(Query query, String field){
        this.query = query;
        this.field = field;
    }
    
    public Query exists(){
        query.append(field, new BasicDBObject("$exists", true));
        return query;
    }
    
    public Query doesNotExist(){
        query.append(field, new BasicDBObject("$exists", false));
        return query;
    }
    
    public Query equal(Object value){
        query.append(field, value);
        return query;
    }
    
    public Query notEqual(Object value){
        query.append(field, new BasicDBObject(NE, value));
        return query;
    }
    
    public Query lessThan(Object value){
        query.append(field, new BasicDBObject(LT, value));
        return query;
    }
    
    public Query lessThanOrEq(Object value){
        query.append(field, new BasicDBObject(LTE, value));
        return query;
    }
    
    public Query greaterThan(Object value){
        query.append(field, new BasicDBObject(GT, value));
        return query;
    }
    
    public Query greaterThanOrEq(Object value){
        query.append(field, new BasicDBObject(GTE, value));
        return query;
    }    
    
    public Query hasOneOf(Iterable<?> value){
        query.append(field, new BasicDBObject("$in", value));
        return query;
    }
    
    public Query hasAllOf(Iterable<?> value){
        query.append(field, new BasicDBObject("$all", value));
        return query;
    }
    
    public Query hasNoneOf(Iterable<?> value){
        query.append(field, new BasicDBObject("$nin", value));
        return query;
    }
    
    public Query hasThisOne(Object value){
        return equal(value);
    }
        
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
