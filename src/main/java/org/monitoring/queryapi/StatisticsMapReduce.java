package org.monitoring.queryapi;

import com.mongodb.BasicDBObject;
import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.DBObject;
import com.mongodb.MapReduceCommand;
import com.mongodb.MapReduceOutput;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * Implementation of statistical functions via Map Reduce of MongoDB
 * Use only if Aggregation Framework is not usable. Map Reduce is slower then Agg.Framew.
 * 
 * @author Michal Dubravcik
 */
public class StatisticsMapReduce implements Statistics {
    
    private Query query;
    
    public StatisticsMapReduce(Query query){
        this.query = query;
    }

    public DBObject count() {
        String map = "count_map(this)";
        String reduce = "function(id,values){ return count_reduce(id, values);}";
        return aggregate(map, reduce, "1", query.getStep());
    }

    public DBObject avg(String field) {
        String map = "map(this)";
        String reduce = "function(id, values){ return avg_reduce(id, values);}";
        return aggregate(map, reduce, field, query.getStep());
    }

    public DBObject sum(String field) {
        String map = "map(this)";
        String reduce = "function(id,values){ return sum_reduce(id,values);}";
        return aggregate(map, reduce, field, query.getStep());
    }

    public DBObject max(String field) {
        String map = "function(){ return map(this);}";
        String reduce = "function(id,values){ return max_reduce(id, values);}";
        return aggregate(map, reduce, field, query.getStep());
    }

    public DBObject min(String field) {
        String map = "map(this)";
        String reduce = "function(id,values){ return min_reduce(id, values);}";
        return aggregate(map, reduce, field, query.getStep());
    }

    public DBObject aggregate(String map, String reduce, String field, long step) {
        Map<String, Object> scope = getScope(field, step);
        return wrap("result", mapReduce(map, reduce, scope));
    }
    
    /**
     * Perform Map Reduce command taking into account order and limit beside parameters
     *
     * @param map map JS function
     * @param reduce reduce JS function
     * @param finalize finalize JS function
     * @param output name of output collection
     * @see MapReduceCommand.OutputType
     * @param type type of job on output
     * @param scope global JS variables
     * @param qStart starting date
     * @param qEnd ending date
     * @param limit maximal number of documents
     */
    protected Iterable<DBObject> mapReduce(String map, String reduce, String finalize, String output,
            MapReduceCommand.OutputType type, Map<String, Object> scope, Date qStart, Date qEnd, int limit) {

        MapReduceCommand mapReduceCmd;

        if (qEnd == null && qStart == null) {
            mapReduceCmd =
                    new MapReduceCommand(query.getCollection(), map, reduce, output, type, query.getMatchQuery());
        } else {
            mapReduceCmd =
                    new MapReduceCommand(query.getCollection(), map, reduce, output, type, query.getMatchQueryWithSubTime(qStart, qEnd));
        }

        if (!finalize.isEmpty()) {
            mapReduceCmd.setFinalize(finalize);
        }
        if (!query.getOrder().keySet().isEmpty()) {
            mapReduceCmd.setSort(query.getOrder());
        } else {
            mapReduceCmd.setSort(new BasicDBObject("_id", 1));
        }

        if (limit != 0) {
            mapReduceCmd.setLimit(limit);
        }

        if (scope != null) {
            mapReduceCmd.setScope(scope);
        }

        MapReduceOutput out = query.getCollection().getDB().getCollection(output).mapReduce(mapReduceCmd);

        //System.out.println(out.getCommandResult());
        //System.out.println(out.getCommand());

        return out.results();
    }

    private Iterable<DBObject> mapReduce(String map, String reduce, String finalize, String output, MapReduceCommand.OutputType type, Map<String, Object> scope, int limit) {
        return mapReduce(map, reduce, finalize, output, type, scope, null, null, limit);
    }

    private Iterable<DBObject> mapReduce(String map, String reduce, Map<String, Object> scope) {
        return mapReduce(map, reduce, "", "", MapReduceCommand.OutputType.INLINE, scope, query.getLimit());
    }
    
    private Map<String, Object> getScope(long step) {
        Map<String, Object> scope = new HashMap<String, Object>();
        scope.put("step", step);
        return scope;
    }

    private Map<String, Object> getScope(String field, long step) {
        Map<String, Object> scope = getScope(step);
        scope.put("field", field);
        return scope;
    }
    
    protected Map<String, Object> getScope(String field, long step, String hash) {
        Map<String, Object> scope = getScope(field, step);
        scope.put("hash", hash);
        return scope;
    }

    
    /**
     * Wrap 2 pairs of (String,object) into new DBObject
     */
    protected DBObject wrap(String firstKey, Object firstValue, String secondKey, Object secondValue) {
        return BasicDBObjectBuilder.start().append(firstKey, firstValue).append(secondKey, secondValue).get();
    }

    /**
     * Wrap pair (String,object) into new DBObject
     */
    protected DBObject wrap(String firstKey, Object firstValue) {
        return new BasicDBObject(firstKey, firstValue);
    }
}
