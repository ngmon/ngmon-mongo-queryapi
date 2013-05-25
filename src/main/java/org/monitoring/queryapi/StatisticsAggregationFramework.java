package org.monitoring.queryapi;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.DBObject;
import java.util.LinkedList;
import java.util.List;

/**
 * Implementation of statistical functions via Aggregation Framework of MongoDB
 * 
 * @author Michal Dubravcik
 */
public class StatisticsAggregationFramework implements Statistics {
    
    private Query query;
    
    
    public StatisticsAggregationFramework(Query query){
        this.query = query;
    }

    @Override
    public DBObject count() {
        return aggregate(new BasicDBObject(Aggregation.SUM, 1));
    }

    @Override
    public DBObject avg(String field) {
        return aggregate(new BasicDBObject(Aggregation.AVG, "$" + field));
    }

    @Override
    public DBObject sum(String field) {
        return aggregate(new BasicDBObject(Aggregation.SUM, "$" + field));
    }

    @Override
    public DBObject min(String field) {
        return aggregate(new BasicDBObject(Aggregation.MIN, "$" + field));
    }

    @Override
    public DBObject max(String field) {
        return aggregate(new BasicDBObject(Aggregation.MAX, "$" + field));
    }
    
    /**
     * Aggregation function. Universal computing via Aggregation Framework.     * 
     * @param appliedAggregationFunction contains DBObject with applied aggregation function on field.
     *  eg. {"$max: "field"} counting maximal value in interval. Can contains multiple functions and fields.
     * @return 
     */
    public DBObject aggregate(DBObject appliedAggregationFunction) {
        DBObject matchOp = new BasicDBObject(Aggregation.MATCH, query.getMatchQuery());
        DBObject groupOp = groupingObjectForAF(appliedAggregationFunction);
        List<DBObject> pipe = new LinkedList<DBObject>();
        pipe.add(groupOp);
        if (!query.getOrder().keySet().isEmpty()) {
            DBObject sortOp = new BasicDBObject(Aggregation.SORT, query.getOrder());
            pipe.add(sortOp);
        }
        if (query.getLimit() != 0) {
            DBObject limitOp = new BasicDBObject(Aggregation.LIMIT, query.getLimit());
            pipe.add(limitOp);
        }
        return wrap("result", query.getCollection().aggregate(matchOp, pipe.toArray(new DBObject[0])).results());
    }


    /**
     * Construct DBObject for group operator for Aggregation Framework
     * @param appliedAggregationFunction
     * @return 
     */
    public DBObject groupingObjectForAF(DBObject appliedAggregationFunction) {
        BasicDBList add = new BasicDBList();
        BasicDBList multiply = new BasicDBList();
        multiply.add(31536000000L);
        multiply.add(new BasicDBObject("$year", "$date"));
        add.add(new BasicDBObject("$multiply", multiply));
        multiply = new BasicDBList();
        multiply.add(86400000);
        multiply.add(new BasicDBObject("$dayOfYear", "$date"));
        add.add(new BasicDBObject("$multiply", multiply));
        multiply = new BasicDBList();
        multiply.add(3600000);
        multiply.add(new BasicDBObject("$hour", "$date"));
        add.add(new BasicDBObject("$multiply", multiply));
        multiply = new BasicDBList();
        multiply.add(60000);
        multiply.add(new BasicDBObject("$minute", "$date"));
        add.add(new BasicDBObject("$multiply", multiply));
        multiply = new BasicDBList();
        multiply.add(1000);
        multiply.add(new BasicDBObject("$second", "$date"));
        add.add(new BasicDBObject("$multiply", multiply));
        add.add(new BasicDBObject("$millisecond", "$date"));
        BasicDBList mod = new BasicDBList();
        mod.add(new BasicDBObject("$add", add));
        mod.add(query.getStep());
        BasicDBList subtract = new BasicDBList();
        subtract.add("$date");
        subtract.add(new BasicDBObject("$mod", mod));
        DBObject group = BasicDBObjectBuilder.start()
                .push("$group")
                .push("_id")
                .add("$subtract", subtract)
                .pop()
                .add("value", appliedAggregationFunction)
                .get();
        return group;
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
