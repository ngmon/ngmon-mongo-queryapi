package org.monitoring.queryapi;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MapReduceCommand;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.Map;

/**
 * Implementation of statistical functions via Map Reduce of MongoDB with caching into seperate 
 * collection
 * Use only if Aggregation Framework is not usable or you want to reuse already computed results
 * Map Reduce with caching is slower then Agg.Framew. and Map Reduce
 * 
 * @author Michal Dubravcik
 */
public class StatisticsMapReduceCached extends StatisticsMapReduce implements Statistics {

    private Query query;
    private CachePointMapper dbmapper = new CachePointMapper();
    public static final String ID_FLAG = CachePointMapper.CACHE_FLAG;
    private final String CACHE = Manager.CACHE;
    private final String CACHE_FLAGS = Manager.CACHE_FLAGS;

    public StatisticsMapReduceCached(Query query) {
        super(query);
        this.query = query;
    }

    public DBObject count() {
        String map = "count_map_cached(this)";
        String reduce = "function(id,values){ return count_reduce(id, values);}";
        return aggregate("", map, reduce);
    }

    public DBObject avg(String field) {
        String map = "map_cached(this)";
        String reduce = "function(id,values){ return avg_reduce(id, values);}";
        return aggregate(field, map, reduce);
    }

    public DBObject sum(String field) {
        String map = "map_cached(this)";
        String reduce = "function(id,values){ return sum_reduce(id, values);}";
        return aggregate(field, map, reduce);
    }

    public DBObject max(String field) {
        String map = "map_cached(this)";
        String reduce = "function(id,values){ return max_reduce(id, values);}";
        return aggregate(field, map, reduce);
    }

    public DBObject min(String field) {
        String map = "map_cached(this)";
        String reduce = "function(id,values){ return min_reduce(id, values);}";
        return aggregate(field, map, reduce);
    }

    public DBObject aggregate(String field, String map, String reduce) {

        String finalize = "";
        /* create cache identifier */
        CacheMatcher cm = new CacheMatcher(reduce, field, query.getMatchQuery().toString(), query.getStep());
        /* bind global variables for map-reduce on serve-side */
        Map<String, Object> scope = getScope(field, query.getStep(), cm.getMD5());
        return cache(cm, map, reduce, finalize, scope);
    }

    /**
     * Perform Map-Reduce with parameters, cache and output the result. Computed results are saved
     * in special collection. At next cache invoking with same identifier and date boundaries
     * (dateFrom(),dateTo()), result is returned from cache. At cache invoking with same identifier
     * and date boundaries intersected with cache date boundaries, map reduce is performed only at
     * not computed date intervals. Finally it returns result from overall interval.
     *
     * @param cm cache identifier
     * @param map JS map function
     * @param reduce JS reduce function
     * @param finalize JS finalize function
     * @param scope global variables for mr server-side
     * @return map reduce result from cache
     */
    private DBObject cache(CacheMatcher cm, String map, String reduce, String finalize, Map<String, Object> scope) {
        Date start = query.getFromDate();
        Date end = query.getToDate();
        if (start == null) {
            Calendar cal = new GregorianCalendar(1970, 0, 0);
            start = cal.getTime();
        }
        if (end == null) {
            Calendar cal = new GregorianCalendar(2050, 0, 0);
            end = cal.getTime();
        }
        final CachePoint CACHE_POINT_START = new CachePoint(start, cm.getOperation(), cm.getField(), query.getMatchQuery().toString(), CachePoint.Flag.START, query.getStep());
        final CachePoint CACHE_POINT_END = new CachePoint(end, cm.getOperation(), cm.getField(), query.getMatchQuery().toString(), CachePoint.Flag.END, query.getStep());

        DBCollection cacheFlags = query.getCollection().getDB().getCollection(CACHE_FLAGS);
        DBCollection cache = query.getCollection().getDB().getCollection(CACHE);

        BasicDBList or = new BasicDBList();
        or.add(BasicDBObjectBuilder.start().push("_id." + CachePoint.ID_TIME)
                .append(Field.NE, start).get());
        or.add(BasicDBObjectBuilder.start().push(ID_FLAG)
                .append(Field.NE, CachePoint.Flag.START.get()).get());

        DBObject match = BasicDBObjectBuilder.start()
                .push("_id.date").append(Field.GTE, start)
                .append(Field.LT, end).pop()
                .append("_id.match", cm.getMD5())
                .append("_id.step", query.getStep())
                .append("$or", or)
                .get();

        DBCursor cursor = cacheFlags.find(match);

        if (cursor.hasNext()) { //not empty response -> partially cached
            while (cursor.hasNext()) {
                CachePoint point1, point2;
                point1 = dbmapper.fromDB(cursor.next());
                if (point1.getFlag() == CachePoint.Flag.START
                        && point1.getDate() == start) {
                } else if (point1.getFlag() == CachePoint.Flag.START) {
                    mapReduce(map, reduce, finalize, CACHE,
                            MapReduceCommand.OutputType.MERGE, scope, start, point1.getDate(), 0);
                } else if (cursor.hasNext()) {
                    point2 = dbmapper.fromDB(cursor.next());
                    mapReduce(map, reduce, finalize, CACHE, MapReduceCommand.OutputType.MERGE, scope, point1.getDate(), point2.getDate(), 0);
                } else {
                    mapReduce(map, reduce, finalize, CACHE, MapReduceCommand.OutputType.MERGE, scope, point1.getDate(), end, 0);
                }
            }

            CachePoint.Flag beforeStart = getInclusiveBeforePoint(start);
            if (beforeStart == CachePoint.Flag.START) {
                //do nothing with start
            } else if (beforeStart == CachePoint.Flag.END) {
                //remove old end
                cacheFlags.remove(new BasicDBObject("_id." + CachePoint.ID_TIME, start));
            } else {
                //insert start
                cacheFlags.save(dbmapper.toDB(CACHE_POINT_START));
            }

            cacheFlags.remove(BasicDBObjectBuilder.start()
                    .push("_id." + CachePoint.ID_TIME)
                    .append(Field.GT, start)
                    .append(Field.LTE, end).get());

            CachePoint.Flag afterEnd = getInclusiveAfterPoint(end);
            if (afterEnd == CachePoint.Flag.END) {
                //do not add end
            } else {
                //insert end
                cacheFlags.save(dbmapper.toDB(CACHE_POINT_END));
            }

        } else { //empty response -> all cached or nothing cached
            if (isStartInclusiveBeforePoint(start)) {
                //all cached, ready for query from cache"
            } else if (getAtPoint(end) == CachePoint.Flag.START) {
                cacheFlags.remove(new BasicDBObject("_id." + CachePoint.ID_TIME, end));
                cacheFlags.save(dbmapper.toDB(CACHE_POINT_START));
                //nothing cached, remove end
                mapReduce(map, reduce, finalize, CACHE, MapReduceCommand.OutputType.MERGE, scope, start, end, 0);
            } else {
                //nothing cached, need to recompute
                mapReduce(map, reduce, finalize, CACHE, MapReduceCommand.OutputType.MERGE, scope, start, end, 0);
                cacheFlags.save(dbmapper.toDB(CACHE_POINT_START));
                cacheFlags.save(dbmapper.toDB(CACHE_POINT_END));
            }
        }
        return wrap("result", cache.find(match).sort(new BasicDBObject("_id", 1)));
    }

    /**
     * Cache assistant method. Get CachePoint flag having date before date specified (inclusively)
     */
    private CachePoint.Flag getInclusiveBeforePoint(Date date) {
        DBCollection cacheflags = query.getCollection().getDB().getCollection(CACHE_FLAGS);
        DBObject beforeStartQuery = BasicDBObjectBuilder.start()
                .push("_id." + CachePoint.ID_TIME)
                .append(Field.LTE, date)
                .get();
        DBObject order = new BasicDBObject("_id." + CachePoint.ID_TIME, -1);
        List<DBObject> beforeStartResponseList = cacheflags
                .find(beforeStartQuery).sort(order).limit(1).toArray();
        if (beforeStartResponseList.isEmpty()) {
            return CachePoint.Flag.NONE;
        } else {
            return dbmapper.fromDB(beforeStartResponseList.get(0)).getFlag();
        }
    }

    private boolean isStartInclusiveBeforePoint(Date date) {
        return getInclusiveBeforePoint(date) == CachePoint.Flag.START;
    }

    /**
     * Cache assistant method. Get CachePoint flag having date after date specified (inclusively)
     */
    private CachePoint.Flag getInclusiveAfterPoint(Date date) {
        DBCollection cacheflags = query.getCollection().getDB().getCollection(CACHE_FLAGS);
        DBObject afterEndQuery = BasicDBObjectBuilder.start()
                .push("_id." + CachePoint.ID_TIME)
                .append(Field.GTE, date).get();
        DBObject order = new BasicDBObject("_id." + CachePoint.ID_TIME, 1);
        List<DBObject> afterEndResponseList = cacheflags.find(afterEndQuery)
                .sort(order).limit(1).toArray();
        if (afterEndResponseList.isEmpty()) {
            return CachePoint.Flag.NONE;
        } else {
            return dbmapper.fromDB(afterEndResponseList.get(0)).getFlag();
        }
    }

    /**
     * Cache assistant method. Get CachePoint flag saved in cache with date specified
     */
    private CachePoint.Flag getAtPoint(Date date) {
        DBCollection cacheflags = query.getCollection().getDB().getCollection(CACHE_FLAGS);
        DBObject atQuery = BasicDBObjectBuilder.start()
                .append("_id." + CachePoint.ID_TIME, date)
                .get();
        List<DBObject> atResponseList = cacheflags.find(atQuery).limit(1).toArray();
        if (atResponseList.isEmpty()) {
            return CachePoint.Flag.NONE;
        } else {
            return dbmapper.fromDB(atResponseList.get(0)).getFlag();
        }
    }
}
