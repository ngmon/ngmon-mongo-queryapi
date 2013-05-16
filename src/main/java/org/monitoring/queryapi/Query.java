package org.monitoring.queryapi;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import java.util.ArrayList;
import java.util.Date;
import org.monitoring.queryapi.Manager.Mode;

/**
 * Class for composing complex queries.
 *
 * @author Michal Dubravcik
 */
public class Query {

    private DBCollection col;
    private BasicDBObjectBuilder query = new BasicDBObjectBuilder();
    private BasicDBObjectBuilder sort = new BasicDBObjectBuilder();
    private int limit = 0;
    private long step = 1000;
    private Date start = null;
    private Date end = null;
    private Statistics stat =
            (Manager.MODE == Manager.Mode.AggregationFramework)
            ? new StatisticsAggregationFramework(this)
            : ((Manager.MODE == Manager.Mode.MapReduce)
            ? new StatisticsMapReduce(this)
            : new StatisticsMapReduceCached(this));
    public static final String ID_DATA = "data";
    public static final String ID_DATE = "date";

    public Query(DBCollection col) {
        this.col = col;
    }

    public DBCollection getCollection() {
        return col;
    }

    /**
     * Append pair {field:value} into matching DBObject
     *
     * @param field
     * @param value
     */
    public void append(String field, Object value) {
        query.add(field, value);
    }

    /**
     * Access field operations on specified field
     *
     * @see Field
     * @param field
     * @return Field
     */
    public Field field(String field) {
        return new Field((Query) this, field);
    }

    /**
     * Sort documents on output by field ascending
     *
     * @param field
     * @return Query for chaining
     */
    public Query orderAsc(String field) {
        sort.append(field, 1);
        return this;
    }

    public Query orderDateAsc() {
        return orderAsc("_id");
    }

    /**
     * Sort documents on output by field descending
     *
     * @param field
     * @return Query for chaining
     */
    public Query orderDesc(String field) {
        sort.append(field, -1);
        return this;
    }

    public Query orderDateDesc() {
        return orderDesc("_id");
    }

    public DBObject getOrder() {
        return sort.get();
    }

    /**
     * Restricts the number of output documents
     *
     * @param num
     * @return Query for chaining
     */
    public Query limit(int num) {
        if (limit < 0) {
            throw new RuntimeException("Limit in Query have to be positive, is :" + limit);
        }
        limit = num;
        return this;
    }

    public int getLimit() {
        return limit;
    }

    public void setMode(Mode mode) {
        this.mode = mode;
    }
    
    public void setImplementation(Statistics stat){
        this.stat = stat;
    }
    
    public Statistics getImplementation(){
        return stat;
    }

    /**
     * Get matching DBObject set by append method, Field operations and date boundaries
     *
     * @return
     */
    public DBObject getMatchQuery() {
        BasicDBObjectBuilder queryLocal = new BasicDBObjectBuilder();
        if (start != null) {
            queryLocal.push(ID_DATE).append(Field.GTE, start);
        }
        if (end != null) {
            if (queryLocal.isEmpty()) {
                queryLocal.push(ID_DATE).append(Field.LT, end);
            } else {
                queryLocal.append(Field.LT, end);
            }
        }
        DBObject out = query.get();
        out.putAll(queryLocal.get());
        return out;
    }

    /**
     * Get matching DBObject set by append method, Field operations and append specified explicit
     * date boundaries - omit date set by dateFrom(),dateTo()
     *
     * @param qStart start boundary of explicit time interval
     * @param qEnd end boundary
     * @return
     */
    public DBObject getMatchQueryWithSubTime(Date qStart, Date qEnd) {
        BasicDBObjectBuilder queryLocal = new BasicDBObjectBuilder();
        if (qStart != null) {
            queryLocal.push(ID_DATE).append(Field.GTE, qStart);
        }
        if (qEnd != null) {
            if (queryLocal.isEmpty()) {
                queryLocal.push(ID_DATE).append(Field.LT, qEnd);
            } else {
                queryLocal.append(Field.LT, qEnd);
            }
        }
        DBObject out = query.get();
        out.putAll(queryLocal.get());
        return out;
    }

    /**
     * Set matching starting date (left time boundary)
     *
     * @param date
     * @return Query for chaining
     */
    public Query fromDate(Date date) {
        start = date;
        return this;
    }

    public Date getFromDate() {
        return start;
    }

    /**
     * Set matching ending date (right time boundary)
     *
     * @param date
     * @return Query for chaining
     */
    public Query toDate(Date date) {
        end = date;
        return this;
    }

    public Date getToDate() {
        return end;
    }

    /**
     * Set length of grouping time interval
     *
     * @param step length of interval in milliseconds
     * @return Query for chaining
     */
    public Query setStep(long step) {
        if (step < 0) {
            throw new RuntimeException("Step in Query have to be positive, is :" + step);
        }
        this.step = step;
        return this;
    }

    public long getStep() {
        return step;
    }

    /**
     * Get documents from DB taking into account match, date boundary, limit and order
     *
     * @return documents in DBObject
     */
    public DBObject find() {
        return wrap("result", col.find(getMatchQuery()).sort(sort.get()).limit(limit));
    }

    /**
     * Get all variants saved in given field value
     *
     * @param field
     * @return Query for chaining
     */
    public DBObject distinct(String field) {
        return wrap("result", col.distinct(field, getMatchQuery()));
    }

    /**
     * Get overall number of matched documents taking into account match and date boundaries
     *
     * @return count
     */
    public int countAll() {
        return col.find(getMatchQuery()).count();
    }

    /**
     * Get number of documents in interval specified by step taking into account match, date
     * boundaries and limit
     *
     * @return DBObject with times and counts in array on key result
     */
    public DBObject count() {
        return stat.count();
    }

    /**
     * Compute average from values on specified field in interval specified by step taking into
     * account match, date boundaries and limit
     *
     * @param field
     * @return DBObject with times and avgs in array on key result
     */
    public DBObject avg(String field) {
        return stat.avg(field);
    }

    /**
     * Compute sum of values on specified field in interval specified by step taking into account
     * match, date boundaries and limit
     *
     * @param field
     * @return DBObject with times and sums in array on key result
     */
    public DBObject sum(String field) {
        return stat.sum(field);
    }

    /**
     * Find minimal value from values of specified field in interval specified by step taking into
     * account match, date boundaries and limit
     *
     * @param field
     * @return DBObject with times and mins in array on key result
     */
    public DBObject min(String field) {
        return stat.min(field);
    }

    /**
     * Find maximal value from values on specified field in interval specified by step taking into
     * account match, date boundaries and limit
     *
     * @param field
     * @return DBObject with times and maxs in array on key result
     */
    public DBObject max(String field) {
        return stat.max(field);
    }

    /**
     * Compute median on specified field in interval specified by step 
     *
     * @param field
     * @return DBObject with times and medians in array on key result
     */
    public DBObject median(String field) { 
        throw new UnsupportedOperationException("Needed to implement on application side");
    }

    /**
     * Find most occured documents having time before time of documents searchable by matching with
     * effect document (designed for finding reasons of effects)
     *
     * @param effect document identifing many documents in db
     * @param limit restrict returned number of documents
     * @param timeBefore time before effects (in milliseconds)
     * @param groupBy fields for grouping documents
     * @return Query for chaining
     */
    public DBObject reasonFor(DBObject effect, int limit, int timeBefore, String... groupBy) {
        BasicDBList dates = new BasicDBList();
        BasicDBObjectBuilder builder = new BasicDBObjectBuilder();
        Iterable<DBObject> reasons;
        int num = 0;

        Iterable<DBObject> results = col.find(effect);

        for (DBObject result : results) {
            dates.add(
                    BasicDBObjectBuilder.start()
                    .push(ID_DATE)
                    .append(Field.LTE, (Date) result.get(ID_DATE))
                    .append(Field.GTE, new Date(((Date) result.get(ID_DATE)).getTime() - timeBefore))
                    .get());
            num++;
        }
        if (num > 0) {
            DBObject match = BasicDBObjectBuilder.start()
                    .append(Aggregation.MATCH, new BasicDBObject(Aggregation.OR, dates))
                    .get();

            for (String groupKey : groupBy) {
                builder.append(groupKey.substring(
                        groupKey.lastIndexOf(".") + 1, groupKey.length()), "$" + groupKey);
            }

            DBObject groupInner = builder.get();

            DBObject group = BasicDBObjectBuilder.start()
                    .push(Aggregation.GROUP).append("_id", groupInner)
                    .push("count").append(Aggregation.SUM, 1)
                    .get();
            DBObject order = BasicDBObjectBuilder.start()
                    .push(Aggregation.SORT).append("count", -1)
                    .get();
            DBObject project = BasicDBObjectBuilder.start()
                    .push(Aggregation.PROJECT)
                    .append("_id", 0).append("count", 1)
                    .append("group", "$_id")
                    .get();
            DBObject limiter = new BasicDBObject(Aggregation.LIMIT, limit);

            reasons = col.aggregate(match, group, order, limiter, project)
                    .results();
        } else {
            reasons = new ArrayList<DBObject>();
        }
        return wrap("founded effects", num, "result", reasons);
    }

    /**
     * Wrap 2 pairs of (String,object) into new DBObject
     */
    private DBObject wrap(String firstKey, Object firstValue, String secondKey, Object secondValue) {
        return BasicDBObjectBuilder.start().append(firstKey, firstValue).append(secondKey, secondValue).get();
    }

    /**
     * Wrap pair (String,object) into new DBObject
     */
    private DBObject wrap(String firstKey, Object firstValue) {
        return new BasicDBObject(firstKey, firstValue);
    }
}
