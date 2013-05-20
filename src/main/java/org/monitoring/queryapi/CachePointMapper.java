package org.monitoring.queryapi;

import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.DBObject;
import java.util.Date;

/**
 *
 * @author Michal Dubravcik
 */
public class CachePointMapper {
    
    public static final String CACHE_FLAG = "f";

    public CachePoint fromDB(DBObject dbobject) {
        CachePoint cpoint = new CachePoint();
        cpoint.setDate((Date) ((DBObject) dbobject.get("_id")).get("t"));
        cpoint.setMatch((String) ((DBObject)dbobject.get("_id")).get("m"));
        cpoint.setGroupTime((Long) ((DBObject)dbobject.get("_id")).get("s"));
        cpoint.setFlag(((Integer) dbobject.get(CACHE_FLAG))==1?CachePoint.Flag.END:CachePoint.Flag.START);
        return cpoint;
    }

    public DBObject toDB(CachePoint cachePoint) {
        return BasicDBObjectBuilder.start()
                .push("_id")
                .append(CachePoint.ID_TIME, cachePoint.getDate())
                .append("m", cachePoint.getMatch())
                .append("s", cachePoint.getGroupTime())
                .pop()
                .append(CACHE_FLAG, cachePoint.getFlag().get())
                .get();
    }
}
